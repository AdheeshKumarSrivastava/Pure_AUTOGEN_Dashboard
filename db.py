from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL

from safety import enforce_select_only


def build_engine() -> Engine:
    host = os.getenv("DB_HOST", "")
    port = int(os.getenv("DB_PORT", "1433"))
    database = os.getenv("DB_NAME", "")
    username = os.getenv("DB_USERNAME", "")
    password = os.getenv("DB_PASSWORD", "")

    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    extra = os.getenv("ODBC_EXTRA_PARAMS", "TrustServerCertificate=yes;Encrypt=no")

    # Convert "a=b;c=d" -> {"a":"b","c":"d"}
    query: Dict[str, str] = {"driver": driver}
    if extra:
        for part in extra.split(";"):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                k, v = part.split("=", 1)
                query[k] = v

    url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )

    # connect timeout helps diagnose hangs
    engine = create_engine(url, pool_pre_ping=True, connect_args={"timeout": 10})
    return engine


def run_sql(engine: Engine, sql: str, params: Optional[Dict[str, Any]] = None, limit: Optional[int] = None) -> pd.DataFrame:
    enforce_select_only(sql)
    q = sql.strip().rstrip(";")
    if limit is not None:
        q = f"SELECT TOP {int(limit)} * FROM ({q}) AS __q"
    with engine.connect() as conn:
        return pd.read_sql(text(q), conn, params=params or {})


def list_tables(engine: Engine) -> pd.DataFrame:
    sql = """
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY s.name, t.name
    """
    return run_sql(engine, sql)


def get_columns(engine: Engine, schema: str, table: str) -> pd.DataFrame:
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
    ORDER BY ORDINAL_POSITION
    """
    return run_sql(engine, sql, params={"schema": schema, "table": table})


def get_row_count(engine: Engine, schema: str, table: str) -> int:
    sql = f"SELECT COUNT(1) AS cnt FROM [{schema}].[{table}]"
    df = run_sql(engine, sql)
    return int(df["cnt"].iloc[0]) if not df.empty else 0


def sample_table(engine: Engine, schema: str, table: str, n: int = 100) -> pd.DataFrame:
    sql = f"SELECT * FROM [{schema}].[{table}]"
    return run_sql(engine, sql, limit=n)