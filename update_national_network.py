"""
Generate national rail additions for the graph pipeline.

This script performs the first three steps of the execution plan:
1. Enrich `stations.jsonl` with national rail metadata.
2. Append/merge national rail platform-to-platform hops into `lines.jsonl`.
3. Add the national HUB and GROUND interchange connectors.

After running this script, `graph_times.py` can be executed to rebuild
`shortest_paths.jsonl` for the combined TFL + national network.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from national_graph import build_national_graph

STATIONS_PATH = Path("stations.jsonl")
LINES_PATH = Path("lines.jsonl")


def load_station_records() -> Dict[str, dict]:
    records: Dict[str, dict] = {}
    if not STATIONS_PATH.exists():
        return records

    with STATIONS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            station_id = data.get("station_id")
            if not station_id:
                continue
            records[station_id] = data

    return records


def write_station_records(records: Dict[str, dict]) -> None:
    with STATIONS_PATH.open("w", encoding="utf-8") as f:
        for station_id in sorted(records):
            f.write(json.dumps(records[station_id]) + "\n")


def load_line_records() -> Tuple[List[dict], Dict[Tuple[str, str, str, str], int]]:
    records: List[dict] = []
    index: Dict[Tuple[str, str, str, str], int] = {}

    if not LINES_PATH.exists():
        return records, index

    with LINES_PATH.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            records.append(data)
            key = (
                data["from_id"],
                data["from_line"],
                data["to_id"],
                data["to_line"],
            )
            index[key] = idx

    return records, index


def write_line_records(records: List[dict]) -> None:
    with LINES_PATH.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def merge_station_metadata(existing: Dict[str, dict], national_stations) -> None:
    """
    Update the in-memory station catalogue with national rail metadata.

    Adds both the station ground node and the associated hub node when absent.
    """
    for station_id, meta in national_stations.items():
        record = existing.get(station_id, {"station_id": station_id})
        record["station_name"] = meta.name or record.get("station_name") or station_id
        record["code"] = meta.three_alpha or record.get("code") or meta.tiploc
        record["latitude"] = meta.latitude
        record["longitude"] = meta.longitude
        record["tiploc"] = meta.tiploc
        if meta.atco_code:
            record["atco_code"] = meta.atco_code
        if meta.naptan_code:
            record["naptan_code"] = meta.naptan_code
        if meta.locality_name:
            record["locality"] = meta.locality_name

        existing[station_id] = record

    for record in existing.values():
        record.setdefault("code", None)
        record.setdefault("latitude", None)
        record.setdefault("longitude", None)


def ensure_unique_station_names(records: Dict[str, dict]) -> None:
    """
    Adjust station names so that every human-readable name is unique.

    When duplicates are detected, the first entry keeps the original name.
    Subsequent entries receive a suffix derived from the station metadata,
    and hub nodes are explicitly labelled.
    """
    name_groups: Dict[str, List[str]] = {}
    for station_id, record in records.items():
        name = record.get("station_name") or station_id
        record["station_name"] = name
        key = name.casefold()
        name_groups.setdefault(key, []).append(station_id)

    for station_ids in name_groups.values():
        if len(station_ids) <= 1:
            continue

        seen: set[str] = set()
        for idx, station_id in enumerate(sorted(station_ids)):
            record = records[station_id]
            base = record.get("station_name") or station_id

            if idx == 0:
                new_name = base
            else:
                suffix = record.get("code") or record.get("tiploc") or station_id
                new_name = f"{base} [{suffix}]"

            # Ensure uniqueness even if suffixes collide.
            candidate = new_name
            counter = 1
            while candidate.casefold() in seen:
                candidate = f"{new_name} #{counter}"
                counter += 1

            record["station_name"] = candidate
            seen.add(candidate.casefold())


def filter_ground_nodes(records: Dict[str, dict]) -> None:
    """Remove non-ground (hub) nodes from the station catalogue."""
    to_remove = [station_id for station_id in records if station_id.startswith("HUB")]
    for station_id in to_remove:
        records.pop(station_id, None)


def merge_line_edges(
    records: List[dict],
    index: Dict[Tuple[str, str, str, str], int],
    new_edges: Iterable[dict],
) -> Tuple[int, int]:
    """
    Merge the supplied edges into the existing lines list.

    Returns a tuple of (updates, inserts).
    """
    updates = inserts = 0

    for edge in new_edges:
        key = (
            edge["from_id"],
            edge["from_line"],
            edge["to_id"],
            edge["to_line"],
        )
        time = int(edge["time"])

        if key in index:
            idx = index[key]
            if time < int(records[idx]["time"]):
                records[idx]["time"] = time
                updates += 1
            continue

        records.append(
            {
                "from_id": edge["from_id"],
                "from_line": edge["from_line"],
                "to_id": edge["to_id"],
                "to_line": edge["to_line"],
                "time": time,
            }
        )
        index[key] = len(records) - 1
        inserts += 1

    return updates, inserts


def main() -> None:
    limit_env = os.environ.get("NATIONAL_SCHEDULE_LIMIT")
    limit = int(limit_env) if limit_env else None

    print("Building national rail edges")
    national_result = build_national_graph(limit=limit)

    print("Updating stations.jsonl")
    station_records = load_station_records()
    merge_station_metadata(station_records, national_result.stations)
    filter_ground_nodes(station_records)
    ensure_unique_station_names(station_records)
    write_station_records(station_records)

    print("Updating lines.jsonl")
    line_records, line_index = load_line_records()
    updates, inserts = merge_line_edges(line_records, line_index, national_result.edges)
    write_line_records(line_records)

    print(f"Edge updates: {updates}, new edges: {inserts}")
    print(f"Total stations: {len(station_records)}, total edges: {len(line_records)}")


if __name__ == "__main__":
    main()
