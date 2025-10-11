import os
import sys
from typing import Final, NamedTuple
from collections import defaultdict
from itertools import permutations
from requests.adapters import HTTPAdapter, Retry

import pandas as pd
import requests
from tqdm import tqdm


class GraphEdge(NamedTuple):
    from_station: str
    to_station: str
    time: int


class Station(NamedTuple):
    station_id: str
    station_name: str
    line_id: str
    hub_id: str | None


class StationTimeInterval(NamedTuple):
    id: str
    min_time: int
    max_time: int


class TflAPI:
    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key
        self.base_url = "https://api.tfl.gov.uk"
        self.session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 502, 503, 504 ])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def _fetch_tfl_data(self, endpoint, params=None) -> dict | list | None:
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params.update({"app_id": self.app_id, "app_key": self.app_key})
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"\nHTTP Error {e}")
        except Exception as e:
            print(f"\nError fetching {url}: {str(e)}")
        return None

    def get_modes(self) -> list[str]:
        mode_response = self._fetch_tfl_data("/Line/Meta/Modes")
        modes: list[str] = []
        if mode_response is None:
            return modes

        modes = [x.get("modeName", None) for x in mode_response]
        return modes

    def get_lines(self, modes: list[str]) -> dict[str, str]:
        lines = self._fetch_tfl_data("/Line/Mode/" + ",".join(modes))
        line_d: dict[str, str] = {}
        if lines is None:
            return line_d

        for line in lines:
            line_d[line["id"]] = line["name"]
        return line_d

    def get_ordered_stations(self, line_id: str, direction: str) -> list[list[str]]:
        ordered_stations = self._fetch_tfl_data(
            f"/Line/{line_id}/Route/Sequence/{direction}"
        )

        if not isinstance(ordered_stations, dict):
            return []

        orderedLineRoutes = ordered_stations.get("orderedLineRoutes", [])
        line_stations = []
        for route_variation in orderedLineRoutes:
            station_variation = []
            for station_id in route_variation["naptanIds"]:
                station_variation.append(station_id)
            line_stations.append(station_variation)
        return line_stations

    def get_stations(self, line_id: str) -> dict[str, Station]:
        stations = self._fetch_tfl_data(
            f"/Line/{line_id}/StopPoints?tflOperatedNationalRailStationsOnly=false"
        )
        stations_d: dict[str, Station] = {}

        if stations is None:
            return stations_d

        for station in stations:
            name = station["commonName"]
            id = station["stationNaptan"]
            hub_id = station.get("hubNaptanCode", None)
            stations_d[id] = Station(station_name=name, station_id=id, line_id=line_id, hub_id=hub_id)

        return stations_d

    def get_travel_time(self, from_station: str, to_station: str) -> int:
        timetable = self._fetch_tfl_data(
            f"/Journey/JourneyResults/{from_station}/to/{to_station}?useRealTimeLiveArrivals=false"
        )
        time = -1
        if not isinstance(timetable, dict):
            return time
        durations = []
        for journey in timetable.get("journeys", []):
            durations.append(journey["duration"])
        return min(durations)

    def get_first_stations(self, line_id: str, direction: str) -> list[str]:
        ordered_stations = self._fetch_tfl_data(
            f"/Line/{line_id}/Route/Sequence/{direction}"
        )

        if not isinstance(ordered_stations, dict):
            return []

        first_stations = []
        if ordered_stations is None:
            return first_stations

        orderedLineRoutes = ordered_stations.get("orderedLineRoutes", [])
        for variation in orderedLineRoutes:
            first = variation.get("naptanIds", [None])[0]
            if first is not None:
                first_stations.append(first)
        return first_stations

    def _time_from_journey(self, journey: dict) -> int:
        return int(journey["hour"]) * 60 + int(journey["minute"])

    def get_time_between_trains_at_station(self, line_id: str, station_id: str):
        timetables = self._fetch_tfl_data(f"/Line/{line_id}/Timetable/{station_id}")
        if timetables is None:
            return []
        if not isinstance(timetables, dict):
            return []

        gaps_between_trains: list[int] = self.get_time_between_trains_at_station1(
            timetables
        )
        gaps_between_trains.extend(self.get_time_between_trains_at_station2(timetables))
        return gaps_between_trains

    def get_time_between_trains_at_station1(self, timetables):
        gaps_between_trains: list[int] = []

        routes = timetables.get("timetable", {}).get("routes", [])
        for route in routes:
            schedules = route.get("schedules", [])
            for schedule in schedules:
                journeys = schedule.get("knownJourneys", [])
                previous_journey = None
                for journey in journeys:
                    mins_past_midnight = self._time_from_journey(journey)
                    if previous_journey:
                        gap = mins_past_midnight - previous_journey
                        if gap > 0:
                            gaps_between_trains.append(gap)
                    previous_journey = mins_past_midnight
        return gaps_between_trains

    def get_time_between_trains_at_station2(self, timetables):
        gaps_between_trains: list[int] = []

        routes = timetables.get("timetable", {}).get("routes", [])
        for route in routes:
            schedules = route.get("schedules", [])
            for schedule in schedules:
                periods = schedule.get("periods", [])
                for period in periods:
                    high = period.get("frequency", {}).get("highestFrequency", None)
                    low = period.get("frequency", {}).get("lowestFrequency", None)
                    if high is not None:
                        gaps_between_trains.append(high)
                    if low is not None:
                        gaps_between_trains.append(low)
        return gaps_between_trains

    def get_timetables(
        self, line_id: str, station_id: str
    ) -> list[list[StationTimeInterval]]:
        timetables = self._fetch_tfl_data(
            f"/Line/{line_id}/Timetable/{station_id}?direction=outbound"
        )
        times: list[list[StationTimeInterval]] = []
        if timetables is None:
            return times

        return times


DESIRED_MODES: Final = ["dlr", "elizabeth-line", "overground", "tube", "tram"]

if __name__ == "__main__":
    app_id = os.environ.get("TFL_APP_ID")
    app_key = os.environ.get("TFL_APP_KEY")
    print(f"Using TFL App ID = {app_id}")
    tfl = TflAPI(app_id, app_key)
    modes = tfl.get_modes()
    # Check that the modes we expect are available
    if not set(DESIRED_MODES).issubset(modes):
        print(
            f"Unexpected Travel Mode detected:\nRequested = {DESIRED_MODES}\nRecieved from TFL = {modes}"
        )
        sys.exit(-1)
    line_id_2_name = tfl.get_lines(DESIRED_MODES)

    all_stations = []
    all_routes = []
    all_stops: list[dict[str, str | int]] = []

    for line_id in line_id_2_name.keys():
        print(f"Processing line {line_id}: {line_id_2_name[line_id]}")
        line_edges = set()
        skipped = 0
        station_d = tfl.get_stations(line_id)
        all_stations.extend(station_d.values())

        for direction in ["inbound", "outbound"]:
            ordered_stations = tfl.get_ordered_stations(line_id, direction)

            for ordered_station in ordered_stations:
                route = {}
                route["line_id"] = line_id
                route["stations"] = ordered_station
                all_routes.append(route)
                for from_station, to_station in tqdm(
                    zip(ordered_station, ordered_station[1:]),
                    total=len(ordered_station) - 1,
                ):
                    if (from_station, to_station) in line_edges:
                        skipped += 1
                        continue
                    time = tfl.get_travel_time(from_station, to_station)
                    one_stop = {}
                    one_stop["from_station"] = from_station
                    one_stop["to_station"] = to_station
                    one_stop["time"] = time
                    one_stop["from_line"] = line_id
                    one_stop["to_line"] = line_id
                    all_stops.append(one_stop)
                    line_edges.add((from_station, to_station))
        print(f"Done, saved {skipped} tfl API requests")

    station_lines = defaultdict(list)
    inter_stations = []
    for station in all_stations:
        station_lines[station.station_id].append(station.line_id)
        station_lines[station.station_id].append("GROUND") # Add Ground for station entry/exit


    # TODO: create station - staion within each hub, expand to station-line - station-line in each hub, and add an edge
    #hubs
    hubs = defaultdict(set)
    for station in all_stations:
        if hub := station.hub_id:
            hubs[hub].add(station.station_id)



    for station_id, lines in station_lines.items():
        if len(lines) <= 1:
            continue
        for from_line, to_line in permutations(lines, 2):
            time = 5 # TODO figure out a better way to calculate time
            inter_station = {"station": station_id, "from_line": from_line, "to_line": to_line, "time": time}
            inter_stations.append(inter_station)


    lines_df = pd.DataFrame(line_id_2_name.items(), columns=["line_id", "line_name"])
    lines_df.to_json("lines.jsonl", orient="records", lines=True)

    stations_df = pd.DataFrame(all_stations)
    stations_df.to_json("stations.jsonl", orient="records", lines=True)

    routes_df = pd.DataFrame(all_routes)
    routes_df.to_json("routes.jsonl", orient="records", lines=True)

    times_df = pd.DataFrame(all_stops)
    times_df.to_json("times.jsonl", orient="records", lines=True)

    transfers_df = pd.DataFrame(inter_stations)
    transfers_df.to_json("transfers.jsonl", orient="records", lines=True)
