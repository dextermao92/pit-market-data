"""On-disk layout and atomic writes shared by every dataset.

raw/source=<source>/ingest_date=D/<dataset>/<ingested_at>.json            (API responses)
raw/source=<source>/ingest_date=D/<dataset>/<ingested_at>__<file name>    (files kept as served, e.g. SEC zips)
staged/<dataset>/ingest_date=D/<ingested_at>.parquet       (1:1 with raw)
quarantine/<dataset>/ingest_date=D/<ingested_at>.parquet   (1:1 with raw, only if rows failed)
"""

import json
from datetime import datetime
from pathlib import Path

import polars as pl

TS = pl.Datetime("us", "UTC")


def write_raw(data_dir: Path, source: str, dataset: str, payload: dict, ingested_at: datetime) -> Path:
    path = (data_dir / "raw" / f"source={source}" / f"ingest_date={ingested_at:%Y-%m-%d}" / dataset
            / f"{ingested_at:%Y%m%dT%H%M%S%fZ}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
    tmp.replace(path)  # atomic: a crash never leaves a half-written file under the final name
    return path


def write_raw_bytes(data_dir: Path, source: str, dataset: str, name: str, content: bytes, ingested_at: datetime) -> Path:
    """A file kept exactly as the source served it (zip, CSV...), named <ingested_at>__<name> so names still sort by
    fetch time and the source's own file name is kept."""
    path = (data_dir / "raw" / f"source={source}" / f"ingest_date={ingested_at:%Y-%m-%d}" / dataset
            / f"{ingested_at:%Y%m%dT%H%M%S%fZ}__{name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(path)
    return path


def staged_path(raw_path: Path, layer: str = "staged", dataset: str | None = None) -> Path:
    """dataset: a second staged dataset derived from the same raw files (e.g. corporate_events)."""
    data_dir, ingest_date = raw_path.parents[4], raw_path.parents[1].name
    stem = raw_path.name.split(".", 1)[0]  # <ingested_at>[__<source file name without extension>]
    return data_dir / layer / (dataset or raw_path.parent.name) / ingest_date / f"{stem}.parquet"


def write_staged(raw_path: Path, staged: pl.DataFrame, quarantined: pl.DataFrame, dataset: str | None = None) -> Path:
    q = staged_path(raw_path, "quarantine", dataset)
    if quarantined.height:
        write_parquet(quarantined, q)
    else:
        q.unlink(missing_ok=True)  # a re-stage under fixed rules may clear an earlier quarantine
    return write_parquet(staged, staged_path(raw_path, dataset=dataset))


def write_parquet(df: pl.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.write_parquet(tmp)
    tmp.replace(path)
    return path
