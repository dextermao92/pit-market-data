"""On-disk layout and atomic writes shared by every dataset.

raw/source=<source>/ingest_date=D/<dataset>/<ingested_at>.json
staged/<dataset>/ingest_date=D/<ingested_at>.parquet   (1:1 with raw)
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
    tmp.write_text(json.dumps(payload, default=str))
    tmp.replace(path)  # atomic: a crash never leaves a half-written file under the final name
    return path


def staged_path(raw_path: Path) -> Path:
    data_dir, ingest_date, dataset = raw_path.parents[4], raw_path.parents[1].name, raw_path.parent.name
    return data_dir / "staged" / dataset / ingest_date / f"{raw_path.stem}.parquet"


def write_parquet(df: pl.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.write_parquet(tmp)
    tmp.replace(path)
    return path
