# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
#     "tqdm"
# ]
# ///


import sqlite3
import requests
import time as tm  # Renamed to avoid conflict
import heapq
from tqdm import tqdm
from requests.exceptions import Timeout, RequestException
from collections import defaultdict
APP_ID = os.environ.get["TFL_APP_ID"]
APP_KEY = os.environ.get["TFL_APP_KEY"]

DB_NAME = "london_transport_graph.db"

MODES = ["tube", "dlr", "overground", "elizabeth-line"]
TRANSFER_TIME = 5
BASE_URL = "https://api.tfl.gov.uk"

OVERGROUND_LINES = {
    "london-overground": {
        "lioness": "Watford Junction to Euston",
        "mildmay": "North London Line",
        "suffragette": "Gospel Oak to Barking",
        "weaver": "West London Line",
        "windrush": "Clapham Junction to Surrey Quays",
        "liberty": "Romford to Upminster"
    }
}

conn = sqlite3.connect(DB_NAME)
conn.execute("PRAGMA foreign_keys = ON")
cursor = conn.cursor()

# Clear existing tables
cursor.executescript('''
    DROP TABLE IF EXISTS stations;
    DROP TABLE IF EXISTS station_lines;
    DROP TABLE IF EXISTS adjacent_stations;
    DROP TABLE IF EXISTS journey_times;
''')

# Create tables with correct schema
cursor.executescript('''
    CREATE TABLE stations (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        parent_id TEXT
    ) STRICT;

    CREATE TABLE station_lines (
        station_id TEXT NOT NULL REFERENCES stations(id),
        line_id TEXT NOT NULL,
        PRIMARY KEY(station_id, line_id)
    ) STRICT;

    CREATE TABLE adjacent_stations (
        line_id TEXT NOT NULL,
        from_station TEXT NOT NULL REFERENCES stations(id),
        to_station TEXT NOT NULL REFERENCES stations(id),
        duration INTEGER NOT NULL CHECK(duration > 0),
        PRIMARY KEY(line_id, from_station, to_station)
    ) STRICT;

    CREATE TABLE journey_times (
        origin TEXT NOT NULL REFERENCES stations(id),
        destination TEXT NOT NULL REFERENCES stations(id),
        duration INTEGER NOT NULL CHECK(duration > 0),
        PRIMARY KEY(origin, destination)
    ) STRICT;
''')

def get_tfl(endpoint, params=None):
    """API caller with retry logic and rate limiting"""
    params = params or {}
    params.update({"app_id": APP_ID, "app_key": APP_KEY})

    for attempt in range(3):
        try:
            response = requests.get(
                f"{BASE_URL}{endpoint}",
                params=params,
                timeout=15,
                headers={'Cache-Control': 'no-cache'}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == 2:
                print(f"API Error: {str(e)}")
                return None
            tm.sleep(2 ** attempt)  # Use renamed time module
    return None

def populate_stations_and_lines():
    """Collect station data with proper parent/child relationships"""
    print("Fetching stations and lines...")
    all_stations = []
    all_lines = []
    seen_parents = set()

    for mode in tqdm(MODES, desc="Processing modes"):
        if lines := get_tfl(f"/Line/Mode/{mode}"):
            for line in tqdm(lines, desc=f"{mode} lines", leave=False):
                line_id = line["id"]

                # Handle Overground line naming
                if mode == "overground":
                    for new_name, data in OVERGROUND_LINES["london-overground"].items():
                        if data.split("(")[0].strip().lower() in line["name"].lower():
                            line_id = new_name

                if stations := get_tfl(f"/Line/{line_id}/StopPoints"):
                    for station in stations:
                        parent_id = station.get("stationNaptan") or station.get("naptanId")
                        platform_id = station["naptanId"]
                        name = station["commonName"]

                        # Clean station name
                        for suffix in ["Underground Station", "Rail Station",
                                     "DLR Station", "Platform"]:
                            name = name.replace(suffix, "").strip()

                        # Add parent station
                        if parent_id and parent_id not in seen_parents:
                            all_stations.append((parent_id, name, None))
                            seen_parents.add(parent_id)

                        # Add platform
                        all_stations.append((platform_id, name, parent_id))
                        all_lines.append((platform_id, line_id))

    # Insert stations with proper schema
    unique_stations = list({(s[0], s[1], s[2]) for s in all_stations})
    cursor.executemany(
        "INSERT OR IGNORE INTO stations VALUES (?, ?, ?)",
        unique_stations
    )

    # Insert lines
    cursor.executemany(
        "INSERT OR IGNORE INTO station_lines VALUES (?, ?)",
        [(s[0], line_id) for s, line_id in zip(all_stations, [l[1] for l in all_lines])]
    )

    conn.commit()

def get_line_route(line_id):
    """Get station sequence for a line"""
    if line_id in OVERGROUND_LINES["london-overground"]:
        line_id = "london-overground"

    route_data = get_tfl(f"/Line/{line_id}/Route/Sequence/outbound") or {}

    stations = []
    parent_ids = set()

    for seq in route_data.get("stopPointSequences", []):
        if "stopPoint" in seq:
            for s in seq["stopPoint"]:
                station_id = s.get("stationNaptan") or s.get("naptanId")
                if station_id and station_id not in parent_ids:
                    stations.append(station_id)
                    parent_ids.add(station_id)

    return stations

def get_segment_duration(line_id, from_id, to_id):
    """Calculate travel time between adjacent stations"""
    # Check if same parent station
    cursor.execute("SELECT parent_id FROM stations WHERE id=?", (from_id,))
    from_parent = cursor.fetchone()[0]
    cursor.execute("SELECT parent_id FROM stations WHERE id=?", (to_id,))
    to_parent = cursor.fetchone()[0]

    if from_parent and from_parent == to_parent:
        return TRANSFER_TIME

    # Try journey planner API
    params = {
        "mode": "national-rail,tube,overground,dlr",
        "journeyPreference": "LeastTime",
        "alternativeCycle": "false"
    }

    if journey := get_tfl(f"/Journey/JourneyResults/{from_id}/to/{to_id}", params):
        if journeys := journey.get("journeys"):
            # Find first rail-based journey
            for j in journeys:
                if any(leg["mode"]["id"] != "walking" for leg in j["legs"]):
                    return j["duration"]

    # Fallback to line averages
    line_info = get_tfl(f"/Line/{line_id}")
    if line_info and "routeSections" in line_info:
        avg_speed = line_info["routeSections"][0].get("averageSpeed", 40)
        distance = line_info["routeSections"][0].get("distance", 2)
        return int((distance / avg_speed) * 60)

    # Default value
    return 3

def build_adjacency_graph():
    """Build station connections with rate limiting"""
    print("Building adjacency graph...")
    cursor.execute("SELECT DISTINCT line_id FROM station_lines")
    lines = [row[0] for row in cursor.fetchall()]

    batch = []
    for line_id in tqdm(lines, desc="Processing lines"):
        tm.sleep(0.5)  # Rate limiting
        stations = get_line_route(line_id)

        for i in range(len(stations)-1):
            from_id, to_id = stations[i], stations[i+1]
            if duration := get_segment_duration(line_id, from_id, to_id):
                batch.extend([
                    (line_id, from_id, to_id, duration),
                    (line_id, to_id, from_id, duration)
                ])

    cursor.executemany('''INSERT INTO adjacent_stations VALUES (?, ?, ?, ?)
                       ON CONFLICT DO NOTHING''', batch)
    conn.commit()

def add_transfers():
    """Add transfer connections between lines"""
    print("Adding transfers...")
    cursor.execute('''
        SELECT s1.station_id, s1.line_id, s2.line_id
        FROM station_lines s1
        JOIN station_lines s2
            ON s1.station_id = s2.station_id
            AND s1.line_id < s2.line_id
    ''')

    transfers = [
        (f"TRANSFER_{line1}_{line2}", station, station, TRANSFER_TIME)
        for station, line1, line2 in cursor.fetchall()
    ]

    cursor.executemany('''INSERT INTO adjacent_stations VALUES (?, ?, ?, ?)
                       ON CONFLICT DO NOTHING''', transfers)
    conn.commit()

def dijkstra(start):
    """Calculate shortest paths from a station"""
    cursor.execute("SELECT id FROM stations")
    stations = [row[0] for row in cursor.fetchall()]
    durations = {s: float('inf') for s in stations}
    durations[start] = 0
    heap = [(0, start)]

    graph = defaultdict(list)
    cursor.execute("SELECT from_station, to_station, duration FROM adjacent_stations")
    for f, t, d in cursor.fetchall():
        graph[f].append((t, d))

    while heap:
        current_duration, current = heapq.heappop(heap)
        if current_duration > durations[current]:
            continue

        for neighbor, duration in graph.get(current, []):
            if (new_duration := current_duration + duration) < durations[neighbor]:
                durations[neighbor] = new_duration
                heapq.heappush(heap, (new_duration, neighbor))

    return durations

def calculate_all_journeys():
    """Precompute all journey times"""
    print("Calculating journey times...")
    cursor.execute("SELECT id FROM stations")
    stations = [row[0] for row in cursor.fetchall()]

    for origin in tqdm(stations, desc="Processing stations"):
        durations = dijkstra(origin)
        batch = [(origin, dest, duration)
                for dest, duration in durations.items()
                if duration != float('inf')]
        cursor.executemany('''INSERT OR REPLACE INTO journey_times
                           VALUES (?, ?, ?)''', batch)

    conn.commit()

if __name__ == "__main__":
    try:
        populate_stations_and_lines()
        build_adjacency_graph()
        add_transfers()
        calculate_all_journeys()
    finally:
        conn.close()
    print("Database build completed!")
