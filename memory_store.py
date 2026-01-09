from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional
import duckdb
import pandas as pd


def _to_jsonable(obj: Any) -> Any:
    """Convert non-JSON-serializable objects into JSON-friendly values."""
    # pandas Timestamp
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    # python datetime/date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # numpy scalar types
    try:
        import numpy as np  # optional dependency via pandas
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
    except Exception:
        pass

    # bytes
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")

    # fallback: stringify
    return str(obj)


def safe_json_dumps(data: Any, *, indent: int = 2) -> str:
    """json.dumps wrapper that handles Timestamps/datetimes safely."""
    return json.dumps(data, indent=indent, default=_to_jsonable)


@dataclass
class MemoryStore:
    base_dir: Path

    def __post_init__(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.json_path = self.base_dir / "memory.json"
        self.duckdb_path = self.base_dir / "cache.duckdb"
        self._init_duckdb()

    def _init_duckdb(self) -> None:
        con = duckdb.connect(str(self.duckdb_path))
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cached_queries (
              cache_key VARCHAR PRIMARY KEY,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              parquet_path VARCHAR
            )
            """
        )
        con.close()

    def load_json(self) -> Dict[str, Any]:
        if not self.json_path.exists():
            return {}
        try:
            return json.loads(self.json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # If file is corrupted/partial, don't crash the app.
            return {}

    def save_json(self, data: Dict[str, Any]) -> None:
        self.json_path.write_text(safe_json_dumps(data, indent=2), encoding="utf-8")

    def cache_df(self, key: str, df: pd.DataFrame) -> str:
        parquet_dir = self.base_dir / "parquet"
        parquet_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = parquet_dir / f"{key}.parquet"
        df.to_parquet(parquet_path, index=False)

        con = duckdb.connect(str(self.duckdb_path))
        con.execute(
            "INSERT OR REPLACE INTO cached_queries(cache_key, parquet_path) VALUES (?, ?)",
            [key, str(parquet_path)],
        )
        con.close()
        return str(parquet_path)

    def load_cached_df(self, key: str) -> Optional[pd.DataFrame]:
        con = duckdb.connect(str(self.duckdb_path))
        rows = con.execute(
            "SELECT parquet_path FROM cached_queries WHERE cache_key = ?",
            [key],
        ).fetchall()
        con.close()

        if not rows:
            return None

        path = rows[0][0]
        return pd.read_parquet(path)