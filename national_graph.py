"""
Build national rail graph edges from timetable data.

This module streams the `toc-full.jsonl` feed, classifies services into
slow/express variants per operator, and emits graph edges compatible with
`lines.jsonl`.  It also derives service headways in order to model the wait
time from a hub onto a specific platform (line).
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from national_loader import (
    StationMetadata,
    build_station_catalogue,
    iter_schedules,
    load_corpus_extract,
    load_stop_points,
)

MIN_RATIO_FOR_SLOW = 0.7
GROUND_TO_HUB_MINUTES = 2
PLATFORM_TO_HUB_MINUTES = 2
MAX_WAIT_MINUTES = 25
MIN_TRAVEL_MINUTES = 0.5  # guard against tiny/invalid connections


@dataclass(frozen=True)
class StopEvent:
    station_id: str
    tiploc: str
    arrival: Optional[float]
    departure: Optional[float]


@dataclass(frozen=True)
class EdgeKey:
    from_id: str
    from_line: str
    to_id: str
    to_line: str


@dataclass
class NationalGraphResult:
    edges: List[dict]
    headways: Dict[Tuple[str, str], float]
    stations: Dict[str, StationMetadata]


class TimeNormalizer:
    """Ensure timetable timestamps are monotonically increasing."""

    def __init__(self) -> None:
        self._day_offset = 0
        self._last_value: Optional[float] = None

    def parse(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None

        token = raw.strip()
        half_minute = token.endswith("H")
        if half_minute:
            token = token[:-1]
        if len(token) < 3 or not token.isdigit():
            return None

        hh, mm = int(token[:-2]), int(token[-2:])
        base_minutes = hh * 60 + mm + (0.5 if half_minute else 0.0)
        minutes = base_minutes + self._day_offset * 24 * 60

        if self._last_value is not None and minutes + 1e-6 < self._last_value:
            # Rolled over to the next day inside the same schedule.
            self._day_offset += 1
            minutes = base_minutes + self._day_offset * 24 * 60

        if self._last_value is None or minutes > self._last_value:
            self._last_value = minutes
        return minutes


def station_id_from_tiploc(tiploc: str) -> str:
    """Normalise TIPLOC into a graph station id."""
    return f"910G{tiploc.upper()}"


LONDON_LAT_RANGE = (50.5, 52.2)
LONDON_LON_RANGE = (-0.6, 0.3)


def hub_id_for_station(meta: StationMetadata) -> str:
    """
    Resolve a hub identifier for a station.

    Use the shared HUB<CRS> convention only for locations within the London
    bounding box; otherwise fall back to a namespaced national hub id to avoid
    collisions with the pre-existing TFL hubs.
    """
    if (
        meta.three_alpha
        and meta.latitude is not None
        and meta.longitude is not None
        and LONDON_LAT_RANGE[0] <= meta.latitude <= LONDON_LAT_RANGE[1]
        and LONDON_LON_RANGE[0] <= meta.longitude <= LONDON_LON_RANGE[1]
    ):
        return f"HUB{meta.three_alpha.upper()}"

    return f"HUBNR_{meta.tiploc.upper()}"


def classify_service(
    schedule_segment: dict, stop_ratio: float, toc_code: str
) -> Optional[str]:
    """
    Decide which line suffix (slow/express) to use for a service.

    Returns either "slow" or "express"; None signals we should drop
    this service from the graph.
    """
    category = (schedule_segment.get("CIF_train_category") or "").strip().upper()

    # Skip clear non-passenger categories (bus replacements, etc.).
    if category in {"BR", "BS", "DD"}:
        return None

    if stop_ratio >= MIN_RATIO_FOR_SLOW:
        return "slow"

    return "express"


def extract_stop_events(
    schedule_segment: dict, metadata: Dict[str, StationMetadata]
) -> Tuple[List[StopEvent], float]:
    """
    Convert a JsonScheduleV1 `schedule_segment` into a sequence of StopEvent.

    Returns the stop list plus the ratio of public stops vs. all timing points.
    """
    locations: Sequence[dict] = schedule_segment.get("schedule_location") or []
    if not locations:
        return [], 0.0

    normaliser = TimeNormalizer()
    stops: List[StopEvent] = []
    total_points = 0
    public_stops = 0

    for loc in locations:
        tiploc = (loc.get("tiploc_code") or "").strip().upper()
        if not tiploc or tiploc not in metadata:
            continue

        total_points += 1
        has_public_time = bool(
            (loc.get("public_arrival") or loc.get("public_departure"))
        )
        if not has_public_time:
            continue

        public_stops += 1

        arrival = loc.get("arrival") or loc.get("public_arrival")
        departure = loc.get("departure") or loc.get("public_departure")
        arrival_minutes = normaliser.parse(arrival)
        departure_minutes = normaliser.parse(departure)

        if arrival_minutes is None and departure_minutes is None:
            continue

        stop = StopEvent(
            station_id=station_id_from_tiploc(tiploc),
            tiploc=tiploc,
            arrival=arrival_minutes,
            departure=departure_minutes,
        )
        stops.append(stop)

    ratio = (public_stops / total_points) if total_points else 0.0
    return stops, ratio


def compute_headways(
    departures: Dict[Tuple[str, str], List[float]],
) -> Dict[Tuple[str, str], float]:
    """Determine service headway (in minutes) per station-line."""
    headways: Dict[Tuple[str, str], float] = {}

    for key, times in departures.items():
        if len(times) < 2:
            # Fallback to a conservative 30 minute headway.
            headways[key] = 30.0
            continue

        sorted_times = sorted(times)
        diffs = [b - a for a, b in zip(sorted_times, sorted_times[1:]) if b > a]
        # Include wrap-around to next day.
        wrap = (sorted_times[0] + 24 * 60) - sorted_times[-1]
        if wrap > 0:
            diffs.append(wrap)

        if not diffs:
            headways[key] = 30.0
            continue

        headways[key] = min(diffs)

    return headways


def build_national_graph(
    corpus_path: str = "national_data/CORPUSExtract.json",
    stops_path: str = "national_data/Stops.csv",
    timetable_path: str = "national_data/toc-full.jsonl",
    limit: Optional[int] = None,
) -> NationalGraphResult:
    """
    Build graph edges for the national rail network.

    Args:
        corpus_path: path to CORPUS metadata extract.
        stops_path: path to NaPTAN stop list.
        timetable_path: path to timetable schedule feed.
        limit: optional safety valve for development/testing; limits the number
               of processed schedules.
    """
    corpus = load_corpus_extract(corpus_path)
    stops = load_stop_points(stops_path)
    catalogue = build_station_catalogue(corpus, stops)

    edge_costs: Dict[EdgeKey, float] = {}
    departures: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    station_usage: Dict[str, StationMetadata] = {}
    processed = 0

    for schedule in iter_schedules(timetable_path):
        seg = schedule.get("schedule_segment") or {}
        toc_code = (schedule.get("atoc_code") or "").strip().lower()
        if not toc_code:
            continue

        stops_list, stop_ratio = extract_stop_events(seg, catalogue)
        if len(stops_list) < 2:
            continue

        line_variant = classify_service(seg, stop_ratio, toc_code)
        if not line_variant:
            continue

        line_name = f"{toc_code}-{line_variant}"

        for stop in stops_list:
            station_usage.setdefault(stop.station_id, catalogue[stop.tiploc])

        for idx in range(len(stops_list) - 1):
            current_stop = stops_list[idx]
            next_stop = stops_list[idx + 1]

            if current_stop.departure is None or next_stop.arrival is None:
                continue

            travel_minutes = next_stop.arrival - current_stop.departure
            if travel_minutes < MIN_TRAVEL_MINUTES:
                continue

            key = EdgeKey(
                from_id=current_stop.station_id,
                from_line=line_name,
                to_id=next_stop.station_id,
                to_line=line_name,
            )
            existing = edge_costs.get(key)
            if existing is None or travel_minutes < existing:
                edge_costs[key] = travel_minutes
        for stop in stops_list:
            if stop.departure is not None:
                departures[(stop.station_id, line_name)].append(
                    stop.departure % (24 * 60)
                )

        processed += 1
        if limit is not None and processed >= limit:
            break

    headways = compute_headways(departures)

    platform_edges: List[dict] = []
    for key, value in edge_costs.items():
        platform_edges.append(
            {
                "from_id": key.from_id,
                "from_line": key.from_line,
                "to_id": key.to_id,
                "to_line": key.to_line,
                "time": math.ceil(value),
            }
        )

    # Add platform<->hub edges using headways.
    for (station_id, line_name), headway in headways.items():
        if station_id not in station_usage:
            continue

        wait_minutes = min(headway / 2.0, MAX_WAIT_MINUTES)

        platform_edges.append(
            {
                "from_id": station_id,
                "from_line": line_name,
                "to_id": station_id,
                "to_line": "HUB",
                "time": PLATFORM_TO_HUB_MINUTES,
            }
        )
        platform_edges.append(
            {
                "from_id": station_id,
                "from_line": "HUB",
                "to_id": station_id,
                "to_line": line_name,
                "time": math.ceil(wait_minutes),
            }
        )

    # Add ground<->hub connectors for used stations.
    for station_id, meta in station_usage.items():
        platform_edges.extend(
            [
                {
                    "from_id": station_id,
                    "from_line": "GROUND",
                    "to_id": station_id,
                    "to_line": "HUB",
                    "time": GROUND_TO_HUB_MINUTES,
                },
                {
                    "from_id": station_id,
                    "from_line": "HUB",
                    "to_id": station_id,
                    "to_line": "GROUND",
                    "time": GROUND_TO_HUB_MINUTES,
                },
            ]
        )

        # Ensure the hub identifier is represented (even if not in existing data).
        hub_id = hub_id_for_station(meta)
        platform_edges.extend(
            [
                {
                    "from_id": hub_id,
                    "from_line": "HUB",
                    "to_id": station_id,
                    "to_line": "HUB",
                    "time": GROUND_TO_HUB_MINUTES,
                },
                {
                    "from_id": station_id,
                    "from_line": "HUB",
                    "to_id": hub_id,
                    "to_line": "HUB",
                    "time": GROUND_TO_HUB_MINUTES,
                },
            ]
        )

    return NationalGraphResult(
        edges=platform_edges,
        headways=headways,
        stations=station_usage,
    )


__all__ = ["build_national_graph", "NationalGraphResult", "hub_id_for_station"]
