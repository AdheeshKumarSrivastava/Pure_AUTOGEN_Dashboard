from __future__ import annotations

import hashlib
from typing import Dict, List, Tuple, Optional

import pandas as pd
from sqlalchemy.engine import Engine

from safety import enforce_select_only
from memory_store import MemoryStore
from models import SQLArtifact, DatasetSummary, KPIReport, KPIValue, ExecutionBundle

# NEW: planner-driven KPI engine
from kpi_engine import compute_kpis_from_plan


def _mk_cache_key(prefix: str, sql: str) -> str:
    h = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{h}"


def _infer_column_types(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    time_cols: List[str] = []
    num_cols: List[str] = []
    cat_cols: List[str] = []

    for c in df.columns:
        s = df[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            time_cols.append(c)
        elif pd.api.types.is_numeric_dtype(s):
            num_cols.append(c)
        else:
            cat_cols.append(c)

    # additionally: try parse dates from object columns (light heuristic)
    for c in list(cat_cols):
        if df[c].dtype == "object":
            try:
                parsed = pd.to_datetime(df[c], errors="coerce", utc=False)
                if parsed.notna().mean() > 0.8:
                    time_cols.append(c)
                    cat_cols.remove(c)
            except Exception:
                pass

    return time_cols, num_cols, cat_cols


def run_sql_select(engine: Engine, sql: str) -> pd.DataFrame:
    enforce_select_only(sql)
    q = sql.strip().rstrip(";")
    with engine.connect() as conn:
        return pd.read_sql_query(q, conn)


def execute_and_cache_artifacts(
    engine: Engine,
    mem: MemoryStore,
    artifacts: List[SQLArtifact],
    analysis_plan: Optional[Dict] = None,  # NEW: pass AnalysisPlan.model_dump()
    cache_prefix: str = "ds",
    max_rows_preview: int = 50,
) -> Tuple[ExecutionBundle, Dict[str, pd.DataFrame]]:
    dataset_summaries: List[DatasetSummary] = []
    reports: List[KPIReport] = []
    previews: Dict[str, pd.DataFrame] = {}

    # Keep full datasets for planner-driven KPI computation
    # NOTE: If datasets are huge, we can switch to sampling later.
    full_datasets: Dict[str, pd.DataFrame] = {}

    for art in artifacts:
        sql = art.sql.strip()
        enforce_select_only(sql)

        cache_key = art.cache_key or _mk_cache_key(cache_prefix, sql)

        df = run_sql_select(engine, sql)
        full_datasets[art.dataset_name] = df

        parquet_path = mem.cache_df(cache_key, df)

        time_cols, num_cols, cat_cols = _infer_column_types(df)

        ds_sum = DatasetSummary(
            dataset_name=art.dataset_name,
            cache_key=cache_key,
            parquet_path=parquet_path,
            n_rows=int(df.shape[0]),
            n_cols=int(df.shape[1]),
            columns=[str(c) for c in df.columns],
            inferred_time_columns=time_cols,
            inferred_numeric_columns=num_cols,
            inferred_categorical_columns=cat_cols,
        )
        dataset_summaries.append(ds_sum)

        # -------------------------
        # Generic KPI / insights
        # -------------------------
        kpis: List[KPIValue] = []
        highlights: List[str] = []
        risks: List[str] = []

        kpis.append(KPIValue(name="rows", value=int(df.shape[0])))
        kpis.append(KPIValue(name="columns", value=int(df.shape[1])))

        # missingness
        miss = (df.isna().mean() * 100).sort_values(ascending=False)
        top_miss = miss.head(5)
        if not top_miss.empty and float(top_miss.iloc[0]) > 0:
            risks.append(
                "Top missing columns (%): "
                + ", ".join([f"{i}:{float(top_miss[i]):.1f}" for i in top_miss.index])
            )

        # numeric summaries
        if num_cols:
            for c in num_cols[:6]:
                s = pd.to_numeric(df[c], errors="coerce").dropna()
                if len(s) > 0:
                    kpis.append(KPIValue(name=f"{c}__sum", value=float(s.sum())))
                    kpis.append(KPIValue(name=f"{c}__avg", value=float(s.mean())))
                    kpis.append(KPIValue(name=f"{c}__p95", value=float(s.quantile(0.95))))
            highlights.append(f"Detected numeric columns: {', '.join(num_cols[:10])}")

        # time span
        if time_cols:
            c = time_cols[0]
            t = pd.to_datetime(df[c], errors="coerce")
            if t.notna().any():
                highlights.append(f"Time column `{c}` spans {t.min()} → {t.max()}")

        # top categories
        if cat_cols:
            c = cat_cols[0]
            vc = df[c].astype(str).value_counts(dropna=True).head(5)
            highlights.append(f"Top `{c}`: " + ", ".join([f"{k}({v})" for k, v in vc.items()]))

        reports.append(KPIReport(dataset_name=art.dataset_name, kpis=kpis, highlights=highlights, risks=risks))

        # store preview for dashboard builder context (small)
        previews[art.dataset_name] = df.head(max_rows_preview).copy()

    # -------------------------
    # Planner-driven KPI merge
    # -------------------------
    if analysis_plan:
        try:
            computed_map = compute_kpis_from_plan(analysis_plan, full_datasets)
            # merge into existing reports
            report_by_name = {r.dataset_name: r for r in reports}
            for ds_name, computed_list in computed_map.items():
                r = report_by_name.get(ds_name)
                if not r:
                    continue
                for kc in computed_list:
                    r.kpis.append(KPIValue(name=kc.name [kc.name], value=kc.value, note=kc.note))
        except Exception as e:
            # Don't fail execution just because KPI mapping failed
            # Keep generic KPIs and add a risk note to all reports
            for r in reports:
                r.risks.append(f"Planner-driven KPI computation failed: {e}")

    bundle = ExecutionBundle(datasets=dataset_summaries, reports=reports)
    return bundle, previews