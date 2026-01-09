from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class KPIComputed:
    name: str
    value: Any
    note: Optional[str] = None


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def _pick_best_column(df: pd.DataFrame, keywords: List[str], prefer_numeric: bool = True) -> Optional[str]:
    cols = list(df.columns)
    scored: List[Tuple[int, str]] = []
    for c in cols:
        cn = _norm(str(c))
        score = 0
        for kw in keywords:
            if kw in cn:
                score += 3
        # small bonus if exact-ish
        if any(cn == kw for kw in keywords):
            score += 2

        # type preference
        if prefer_numeric and pd.api.types.is_numeric_dtype(df[c]):
            score += 1
        scored.append((score, c))

    scored.sort(reverse=True, key=lambda x: x[0])
    if scored and scored[0][0] > 0:
        return scored[0][1]
    return None


def _safe_agg(df: pd.DataFrame, col: str, agg: str) -> Optional[float]:
    if col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None
    if agg == "sum":
        return float(s.sum())
    if agg == "avg":
        return float(s.mean())
    if agg == "min":
        return float(s.min())
    if agg == "max":
        return float(s.max())
    if agg == "p95":
        return float(s.quantile(0.95))
    return None


def _safe_count_distinct(df: pd.DataFrame, col: str) -> Optional[int]:
    if col not in df.columns:
        return None
    return int(df[col].nunique(dropna=True))


def _safe_count_rows(df: pd.DataFrame) -> int:
    return int(df.shape[0])


def compute_kpis_from_plan(
    plan: Dict[str, Any],
    datasets: Dict[str, pd.DataFrame],
) -> Dict[str, List[KPIComputed]]:
    """
    Computes KPI values for each dataset by using:
    1) formula_hint if it matches a safe mini-grammar
    2) otherwise heuristics based on KPI name/description
    Returns mapping dataset_name -> list[KPIComputed]
    """
    kpis = plan.get("kpis", []) or []
    results: Dict[str, List[KPIComputed]] = {ds: [] for ds in datasets.keys()}

    # --- mini grammar for formula_hint (SAFE, limited) ---
    # Supported:
    #   sum(column)
    #   avg(column)
    #   count_rows()
    #   count_distinct(column)
    hint_re = re.compile(r"^(sum|avg|min|max|p95|count_distinct)\(([^)]+)\)$|^count_rows\(\)$", re.IGNORECASE)

    for ds_name, df in datasets.items():
        out: List[KPIComputed] = []
        for k in kpis:
            k_name = str(k.get("name", "")).strip()
            k_desc = str(k.get("description", "")).strip()
            hint = k.get("formula_hint")

            # 1) formula_hint path
            if isinstance(hint, str) and hint.strip():
                h = hint.strip()
                m = hint_re.match(h)
                if m:
                    if h.lower().startswith("count_rows"):
                        out.append(KPIComputed(name=k_name or "count_rows", value=_safe_count_rows(df), note="from formula_hint"))
                        continue

                    agg = (m.group(1) or "").lower()
                    col = (m.group(2) or "").strip().strip('"').strip("'")
                    if agg == "count_distinct":
                        val = _safe_count_distinct(df, col)
                        if val is not None:
                            out.append(KPIComputed(name=k_name, value=val, note=f"from formula_hint: {h}"))
                            continue
                    else:
                        val = _safe_agg(df, col, agg)
                        if val is not None:
                            out.append(KPIComputed(name=k_name, value=val, note=f"from formula_hint: {h}"))
                            continue
                # if hint present but not parseable, fall through to heuristics

            # 2) heuristic path
            text = _norm(k_name + " " + k_desc)

            # a) counts / active
            if any(w in text for w in ["count", "total", "number of", "volume"]):
                # try customer/user/member id
                id_col = _pick_best_column(df, ["customer", "user", "member", "account", "client", "cust", "userid", "customerid"], prefer_numeric=False)
                if id_col:
                    out.append(KPIComputed(name=k_name, value=_safe_count_distinct(df, id_col), note=f"heuristic distinct on {id_col}"))
                else:
                    out.append(KPIComputed(name=k_name, value=_safe_count_rows(df), note="heuristic row count"))
                continue

            if "active" in text:
                id_col = _pick_best_column(df, ["customer", "user", "member", "account", "client", "cust", "customerid"], prefer_numeric=False)
                if id_col:
                    out.append(KPIComputed(name=k_name, value=_safe_count_distinct(df, id_col), note=f"heuristic active distinct on {id_col}"))
                else:
                    out.append(KPIComputed(name=k_name, value=_safe_count_rows(df), note="heuristic active rows"))
                continue

            # b) money/amount/earn/spend/redeem/points
            if any(w in text for w in ["revenue", "amount", "spend", "sales", "earning", "earn", "redeem", "redemption", "points"]):
                col = _pick_best_column(
                    df,
                    ["amount", "amt", "revenue", "sales", "spend", "earning", "earned", "redeem", "redemption", "points", "point", "value"],
                    prefer_numeric=True,
                )
                if col:
                    # pick sum by default for these
                    val = _safe_agg(df, col, "sum")
                    if val is not None:
                        out.append(KPIComputed(name=k_name, value=val, note=f"heuristic sum({col})"))
                        continue

            # c) default fallback: rows
            out.append(KPIComputed(name=k_name or "rows", value=_safe_count_rows(df), note="fallback rows"))

        results[ds_name] = out

    return results