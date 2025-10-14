"""
Utility functions for loading and normalising national rail data feeds.

These helpers keep the data-handling logic for the large national files in
one place. They provide:
    * CORPUS metadata indexed by TIPLOC.
    * NaPTAN stop points indexed by TIPLOC (with coordinates).
    * Iterators over timetable records from `toc-full.jsonl`.

The goal is to expose TIPLOC-aligned station metadata (name, CRS code,
coordinates, etc.) that can be merged into the existing graph pipeline.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional


@dataclass(frozen=True)
class CorpusRecord:
    """Reference data for a TIPLOC code as provided by CORPUS."""

    tiploc: str
    name: Optional[str]
    three_alpha: Optional[str]
    nalco: Optional[str]
    stanox: Optional[str]
    uic: Optional[str]


@dataclass(frozen=True)
class StopPoint:
    """NaPTAN stop point associated with a TIPLOC."""

    atco_code: str
    naptan_code: Optional[str]
    name: str
    latitude: Optional[float]
    longitude: Optional[float]
    stop_type: Optional[str]
    locality_name: Optional[str]


@dataclass(frozen=True)
class StationMetadata:
    """TIPLOC-indexed station metadata combining CORPUS and NaPTAN details."""

    tiploc: str
    name: Optional[str]
    three_alpha: Optional[str]
    atco_code: Optional[str]
    naptan_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    locality_name: Optional[str]


def load_corpus_extract(path: Path | str) -> Dict[str, CorpusRecord]:
    """
    Load TIPLOC metadata from `CORPUSExtract.json`.

    Returns a dictionary keyed by the uppercase TIPLOC code.
    """
    corpus_path = Path(path)
    with corpus_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict) or "TIPLOCDATA" not in payload:
        raise ValueError(f"Unexpected CORPUS payload structure in {corpus_path}")

    records: Dict[str, CorpusRecord] = {}
    for raw in payload.get("TIPLOCDATA", []):
        if not isinstance(raw, dict):
            continue
        tiploc = raw.get("TIPLOC") or ""
        tiploc = tiploc.strip().upper()
        if not tiploc:
            continue

        # Some records are placeholders with blank descriptions; skip them.
        name = (raw.get("NLCDESC") or "").strip() or None
        three_alpha = (raw.get("3ALPHA") or "").strip() or None
        nalco = (raw.get("NLC") or None)
        if isinstance(nalco, int):
            nalco = str(nalco)
        nalco = (nalco or "").strip() or None
        stanox = (raw.get("STANOX") or "").strip() or None
        uic = (raw.get("UIC") or "").strip() or None

        records[tiploc] = CorpusRecord(
            tiploc=tiploc,
            name=name,
            three_alpha=three_alpha,
            nalco=nalco,
            stanox=stanox,
            uic=uic,
        )

    return records


_LETTER_BLOCK_RE = re.compile(r"[A-Z]{3,}")


def _extract_tiploc_from_atco(atco_code: str) -> Optional[str]:
    """
    Best-effort TIPLOC extraction from an ATCO code.

    The common patterns we see are:
        * `9100{TIPLOC}`
        * `910{TIPLOC}`
        * `{digits}{TIPLOC}{digits}`
    """
    if not atco_code:
        return None

    match = _LETTER_BLOCK_RE.search(atco_code.upper())
    if not match:
        return None
    return match.group(0)


def _parse_float(value: str) -> Optional[float]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def load_stop_points(path: Path | str) -> Dict[str, StopPoint]:
    """
    Load NaPTAN stop points and index them by TIPLOC.

    Multiple NaPTAN entries can refer to the same TIPLOC. We keep the most
    relevant record using a simple heuristic:
        * Prefer rail station records (`StopType == "RLY"`).
        * Otherwise, keep the first entry that provides coordinates.
    """
    stop_path = Path(path)
    with stop_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        by_tiploc: Dict[str, StopPoint] = {}

        for row in reader:
            atco_code = (row.get("ATCOCode") or "").strip()
            if not atco_code:
                continue

            tiploc = _extract_tiploc_from_atco(atco_code)
            if not tiploc:
                continue

            stop_type = (row.get("StopType") or "").strip() or None
            lat = _parse_float(row.get("Latitude") or "")
            lon = _parse_float(row.get("Longitude") or "")
            name = (row.get("CommonName") or row.get("ShortCommonName") or "").strip()
            if not name:
                name = atco_code

            candidate = StopPoint(
                atco_code=atco_code.upper(),
                naptan_code=(row.get("NaptanCode") or "").strip() or None,
                name=name,
                latitude=lat,
                longitude=lon,
                stop_type=stop_type,
                locality_name=(row.get("LocalityName") or "").strip() or None,
            )

            existing = by_tiploc.get(tiploc)
            if existing is None:
                by_tiploc[tiploc] = candidate
                continue

            existing_is_rly = existing.stop_type == "RLY"
            candidate_is_rly = candidate.stop_type == "RLY"
            existing_has_coords = (
                existing.latitude is not None and existing.longitude is not None
            )
            candidate_has_coords = (
                candidate.latitude is not None and candidate.longitude is not None
            )

            # Always upgrade to a record that supplies coordinates if the current
            # selection does not have them (several NaPTAN rail hubs omit lat/lon).
            if not existing_has_coords and candidate_has_coords:
                by_tiploc[tiploc] = candidate
                continue

            # Prefer rail ("RLY") stop points when they improve on the existing
            # record. This avoids discarding a coordinated entry in favour of an
            # un-coordinated rail placeholder.
            if (
                candidate_is_rly
                and not existing_is_rly
                and (candidate_has_coords or not existing_has_coords)
            ):
                by_tiploc[tiploc] = candidate
                continue

        return by_tiploc


def build_station_catalogue(
    corpus: Dict[str, CorpusRecord], stops: Dict[str, StopPoint]
) -> Dict[str, StationMetadata]:
    """
    Merge CORPUS and NaPTAN data, yielding one metadata record per TIPLOC.

    Fields are filled using CORPUS first (for names/CRS codes) and then
    completed with NaPTAN (coordinates and local descriptors).
    """
    catalogue: Dict[str, StationMetadata] = {}
    all_tiplocs = set(corpus) | set(stops)

    for tiploc in sorted(all_tiplocs):
        corpus_entry = corpus.get(tiploc)
        stop_entry = stops.get(tiploc)

        name = None
        if corpus_entry and corpus_entry.name:
            name = corpus_entry.name
        elif stop_entry:
            name = stop_entry.name

        catalogue[tiploc] = StationMetadata(
            tiploc=tiploc,
            name=name,
            three_alpha=corpus_entry.three_alpha if corpus_entry else None,
            atco_code=stop_entry.atco_code if stop_entry else None,
            naptan_code=stop_entry.naptan_code if stop_entry else None,
            latitude=stop_entry.latitude if stop_entry else None,
            longitude=stop_entry.longitude if stop_entry else None,
            locality_name=stop_entry.locality_name if stop_entry else None,
        )

    return catalogue


def iter_timetable_records(path: Path | str) -> Iterator[dict]:
    """
    Stream raw timetable records from `toc-full.jsonl`.

    Each line is a JSON document. Records of interest (e.g. `JsonScheduleV1`)
    can be filtered by the caller.
    """
    timetable_path = Path(path)
    with timetable_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # Skip malformed lines but keep processing.
                continue


def iter_schedules(path: Path | str) -> Iterator[dict]:
    """
    Convenience wrapper that yields only `JsonScheduleV1` payloads from the
    timetable feed.
    """
    for record in iter_timetable_records(path):
        schedule = record.get("JsonScheduleV1")
        if schedule:
            yield schedule


def iter_tiploc_updates(path: Path | str) -> Iterator[dict]:
    """
    Convenience wrapper that yields only `TiplocV1` update payloads.
    """
    for record in iter_timetable_records(path):
        tiploc_update = record.get("TiplocV1")
        if tiploc_update:
            yield tiploc_update


__all__ = [
    "CorpusRecord",
    "StopPoint",
    "StationMetadata",
    "load_corpus_extract",
    "load_stop_points",
    "build_station_catalogue",
    "iter_timetable_records",
    "iter_schedules",
    "iter_tiploc_updates",
]
