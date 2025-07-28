import os
import sys
from typing import Final, NamedTuple
from requests.adapters import HTTPAdapter, Retry
import concurrent.futures

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
    hub_id: str
    hub_name: str | None


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
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

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

    def get_stop_points(self, stop_point_ids: list[str]) -> list[tuple[str, str]]:
        stops: list[tuple[str, str]] = []

        def batchify(iterable, n=10):
            l = len(iterable)
            for ndx in range(0, l, n):
                yield iterable[ndx : min(ndx + n, l)]

        for batch in batchify(stop_point_ids):
            tfl_stops = self._fetch_tfl_data(f"/StopPoint/{','.join(batch)}")
            if tfl_stops is None:
                print(f"ERROR: Retrieved No Stop Points for {stop_point_ids}")
                continue

            for stop in tfl_stops:
                stops.append((stop["hubNaptanCode"], stop["commonName"]))
        return stops

    def get_stations(self, line_id: str) -> dict[str, Station]:
        stations = self._fetch_tfl_data(
            f"/Line/{line_id}/StopPoints?tflOperatedNationalRailStationsOnly=false"
        )
        stations_d: dict[str, Station] = {}

        if stations is None:
            print(f"ERROR: Retrieved No Stations for {line_id}")
            return stations_d

        for station in stations:
            name = station["commonName"]
            id = station["stationNaptan"]
            if "hubNaptanCode" in station:
                hub_id = station["hubNaptanCode"]
                hub_name = None
            else:
                hub_id = id
                hub_name = name

            stations_d[id] = Station(
                station_name=name,
                station_id=id,
                line_id=line_id,
                hub_id=hub_id,
                hub_name=hub_name,
            )

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

    def get_time_between_trains_at_station1(self, timetables) -> list[int]:
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

    def get_time_between_trains_at_station2(self, timetables) -> list[int]:
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

    def _edge_time(self, edge: tuple[tuple[str,str], tuple[str, str]]) -> tuple[str, str, str, str, int]:
        from_node = edge[0]
        to_node = edge[1]
        if from_node[1] == "GROUND" or to_node[1] == "GROUND":
            return (*from_node, *to_node, 2)
        if from_node[1] == "HUB" or to_node[1] == "HUB":
            return (*from_node, *to_node, 2)
        return (*from_node, *to_node, self.get_travel_time(from_node[0], to_node[0]))

    def edge_processor( self, edges) -> list[tuple[str, str, str, str, int]]:
        timed_edges = []
        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Start the load operations and mark each future with its URL
            future_to_edge = {executor.submit(self._edge_time, edge) for edge in edges}
            for future in tqdm(concurrent.futures.as_completed(future_to_edge), total=len(edges)):
                timed_edges.append(future.result())
        return timed_edges


DESIRED_MODES: Final = ["dlr", "elizabeth-line", "overground", "tube", "tram"]

if __name__ == "__main__":
    app_id = os.environ.get("TFL_APP_ID")
    app_key = os.environ.get("TFL_APP_KEY")
    print(f"Using TFL App ID = {app_id}")

    tfl = TflAPI(app_id, app_key)
    print("Fetching all Valid TFL Modes")
    modes = tfl.get_modes()
    # Check that the modes we expect are available
    if not set(DESIRED_MODES).issubset(modes):
        print(
            f"Unexpected Travel Mode detected:\nRequested = {DESIRED_MODES}\nRecieved from TFL = {modes}"
        )
        sys.exit(-1)
    print(f"Fetching Lines that serve modes: {DESIRED_MODES}")
    line_id_2_name = tfl.get_lines(DESIRED_MODES)

    edges: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    hubs: list[dict[str, str]] = []
    unnamed_hubs: set[str] = set()
    grounds: list[dict[str, str]] = []

    for line_id in line_id_2_name.keys():
        print(f"Fetching Stations on the {line_id} line")
        station_d = tfl.get_stations(line_id)

        for station in station_d.values():
            station_node = (station.station_id, station.line_id)
            hub_node = (station.hub_id, "HUB")
            edges.add((station_node, hub_node))
            edges.add((hub_node, station_node))
            if not station.hub_name:
                unnamed_hubs.add(station.hub_id)
            else:
                hubs.append({"hub_id": station.hub_id, "hub_name": station.hub_name})

        for direction in ["inbound", "outbound"]:
            ordered_stations = tfl.get_ordered_stations(line_id, direction)
            for ordered_station in ordered_stations:
                for from_station, to_station in zip(
                    ordered_station, ordered_station[1:]
                ):
                    edges.add(((from_station, line_id), (to_station, line_id)))

    for hub_id, hub_name in tfl.get_stop_points(list(unnamed_hubs)):
        hubs.append({"hub_id": hub_id, "hub_name": hub_name})

    for hub in hubs:
        hub_id = hub["hub_id"]
        hub_name = hub["hub_name"]
        edges.add(((hub_id, "HUB"), (hub_id, "GROUND")))
        edges.add(((hub_id, "GROUND"), (hub_id, "HUB")))
        grounds.append({"station_name": hub_name, "station_id": hub_id})

    print("Fetching all station -> station times")
    timed_edges = tfl.edge_processor(edges)
    lines_df = pd.DataFrame(
        timed_edges, columns=["from_id", "from_line", "to_id", "to_line", "time"]
    )
    lines_df.to_json("lines.jsonl", orient="records", lines=True)

    stations_df = pd.DataFrame(grounds)
    stations_df.to_json("stations.jsonl", orient="records", lines=True)
