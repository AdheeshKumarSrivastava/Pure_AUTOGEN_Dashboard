from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field


# ---------- DB profiling (optional, but helpful for structured storage) ----------
class ColumnInfo(BaseModel):
    name: str
    data_type: str
    is_nullable: Optional[bool] = None
    max_length: Optional[int] = None


class TableInfo(BaseModel):
    schema: str
    table: str
    row_count: Optional[int] = None
    columns: List[ColumnInfo] = Field(default_factory=list)
    # keep samples small + safe; values must be jsonable strings/numbers/bools
    sample_rows: List[Dict[str, Any]] = Field(default_factory=list)


class DBProfile(BaseModel):
    tables: List[TableInfo] = Field(default_factory=list)


# ---------- Task 3: analysis plan (LLM output) ----------
class KPI(BaseModel):
    name: str
    description: str
    formula_hint: Optional[str] = None


class JoinSpec(BaseModel):
    left: str  # "schema.table"
    right: str
    on: List[str]  # ["left_col = right_col", ...]
    join_type: Literal["inner", "left", "right", "full"] = "left"


class AnalysisPlan(BaseModel):
    dashboard_goal: str
    audience: str
    grain: str  # e.g., "daily customer", "monthly program", etc.
    kpis: List[KPI]
    dimensions: List[str] = Field(default_factory=list)
    filters: List[str] = Field(default_factory=list)
    tables_used: List[str] = Field(default_factory=list)  # ["schema.table", ...]
    joins: List[JoinSpec] = Field(default_factory=list)
    charts: List[str] = Field(default_factory=list)


# ---------- Task 4: SQL artifacts (LLM output) ----------
class SQLArtifact(BaseModel):
    dataset_name: str = Field(..., description="Short name like 'rewards_daily' or 'customer_cohort'")
    description: str
    sql: str
    expected_columns: List[str] = Field(default_factory=list)
    cache_key: Optional[str] = None


class SQLBuildOutput(BaseModel):
    artifacts: List[SQLArtifact]


# ---------- Task 5: executed dataset summaries (system computed) ----------
class DatasetSummary(BaseModel):
    dataset_name: str
    cache_key: str
    parquet_path: str
    n_rows: int
    n_cols: int
    columns: List[str]
    inferred_time_columns: List[str] = Field(default_factory=list)
    inferred_numeric_columns: List[str] = Field(default_factory=list)
    inferred_categorical_columns: List[str] = Field(default_factory=list)


class KPIValue(BaseModel):
    name: str
    value: Any
    note: Optional[str] = None


class KPIReport(BaseModel):
    dataset_name: str
    kpis: List[KPIValue] = Field(default_factory=list)
    highlights: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)


class ExecutionBundle(BaseModel):
    datasets: List[DatasetSummary]
    reports: List[KPIReport]