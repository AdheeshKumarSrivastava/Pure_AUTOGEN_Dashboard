from __future__ import annotations

import os
import json
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st
from dotenv import load_dotenv
import pandas as pd

from team_factory import build_team
from db import build_engine, list_tables, get_columns, get_row_count, sample_table
from memory_store import MemoryStore, safe_json_dumps  # <-- IMPORTANT
from prompts import STEPS

# New imports for structured execution
from models import SQLBuildOutput,AnalysisPlan
from llm_json import parse_llm_json
from executor import execute_and_cache_artifacts

from ui_formatter import beautify_step

load_dotenv()

st.set_page_config(page_title="Agentic Analytics Team (Ollama)", layout="wide")


# ---------- helpers ----------
def ensure_session():
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "current_step_idx" not in st.session_state:
        st.session_state.current_step_idx = 0
    if "approved_steps" not in st.session_state:
        st.session_state.approved_steps = set()
    if "db_profile" not in st.session_state:
        st.session_state.db_profile = {}
    if "team_ready" not in st.session_state:
        st.session_state.team_ready = False
    if "dashboard_html" not in st.session_state:
        st.session_state.dashboard_html = None
    if "execution_bundle" not in st.session_state:
        st.session_state.execution_bundle = None
    if "dataset_previews" not in st.session_state:
        st.session_state.dataset_previews = None


def log(step: str, agent: str, content: str):
    st.session_state.logs.append({"step": step, "agent": agent, "content": content})


def render_logs(step_key: str):
    items = [x for x in st.session_state.logs if x["step"] == step_key]
    if not items:
        st.info ("No output yet for this step.")
        return
    for i, it in enumerate(items):
        with st.expander(f"{it['agent']} output #{i+1}", expanded=(i == len(items) - 1)):
            formatted = beautify_step(step_key,it['agent'],it['content'])
            st.markdown(formatted,unsafe_allow_html=False)


async def run_team_stream(team, task: str, step_key: str):
    async for msg in team.run_stream(task=task):
        agent = getattr(msg, "source", None) or getattr(msg, "name", None) or msg.__class__.__name__
        content = getattr(msg, "content", None)
        if content is None:
            content = str(msg)
        log(step_key, str(agent), str(content))


def latest_agent_output(step_key: str, agent_name: Optional[str] = None) -> Optional[str]:
    """
    Robust fetch:
    - If agent_name is provided, match exact OR substring (case-insensitive)
    - If not found, fallback to any message in the step that looks like SQLBuilder JSON (contains "artifacts")
    """
    agent_name_norm = (agent_name or "").strip().lower()

    # 1) Try exact / substring match
    if agent_name:
        for it in reversed(st.session_state.logs):
            if it["step"] != step_key:
                continue
            a = str(it["agent"]).strip().lower()
            if a == agent_name_norm or agent_name_norm in a:
                return it["content"]

    # 2) Fallback: pick latest JSON-ish content for that step
    for it in reversed(st.session_state.logs):
        if it["step"] != step_key:
            continue
        c = str(it["content"])
        if '"artifacts"' in c or "'artifacts'" in c:
            return c

    # 3) Last fallback: any content from that step
    for it in reversed(st.session_state.logs):
        if it["step"] == step_key:
            return str(it["content"])

    return None


def build_db_profile() -> Dict[str, Any]:
    engine = build_engine()
    tables_df = list_tables(engine)

    profile: Dict[str, Any] = {"tables": []}
    for _, row in tables_df.iterrows():
        schema = row["schema_name"]
        table = row["table_name"]
        cols = get_columns(engine, schema, table)
        try:
            cnt = get_row_count(engine, schema, table)
        except Exception:
            cnt = None
        try:
            samp = sample_table(engine, schema, table, n=5000)
        except Exception:
            samp = pd.DataFrame()

        profile["tables"].append(
            {
                "schema": schema,
                "table": table,
                "row_count": cnt,
                "columns": cols.to_dict(orient="records"),
                "sample_rows": samp.head(5000).to_dict(orient="records"),
            }
        )
    return profile


def step_task_prompt(step_key: str, step_goal: str, user_request: str, db_profile: Dict[str, Any]) -> str:
    # Use safe_json_dumps so Timestamp/Date won't crash prompts
    db_json = safe_json_dumps(db_profile, indent=2)[:60000]

    return f"""
You are a multi-agent TEAM executing ONLY: {step_key}
Goal: {step_goal}

Context:
- Model: Ollama deepseek-r1:8b
- Human-in-loop: ALWAYS (user approves each step in UI)
- Database profile (tables/columns/samples) is provided as JSON below.

User dashboard request (may be empty in early steps):
{user_request}

DB_PROFILE_JSON:
{db_json}

Rules:
- Be extremely detailed and practical.
- SQLBuilder must produce SELECT-only SQL (read-only) and MUST output JSON (no extra text).
- Reviewer must end with exactly "APPROVE_STEP" if the step is acceptable, otherwise list specific fixes.
- Do NOT proceed to other steps.
"""

from table_describer import build_table_description_prompt
from llm_json import parse_llm_json

def enrich_tables_with_descriptions(profile: Dict[str, Any], team) -> Dict[str, Any]:
    for table in profile["tables"]:
        if "table_description" in table:
            continue  # already generated

        prompt = build_table_description_prompt(table)

        # Use the SAME team / model
        response = team.run(task=prompt)

        desc = parse_llm_json(response, dict)

        table["table_description"] = desc["description"]
        table["business_meaning"] = desc["business_meaning"]
        table["important_columns"] = desc["important_columns"]
        table["typical_joins"] = desc["typical_joins"]
        table["dashboard_use_cases"] = desc["dashboard_use_cases"]

    return profile


# ---------- UI ----------
ensure_session()

st.title("🧠 Agentic Analytics Team")

left, right = st.columns([0.52, 0.48], gap="large")

with left:
    st.subheader("1) Connection & DB Scan")
    st.caption("Secrets must be in .env. Do NOT hardcode passwords.")

    if st.button("Scan DB (schema + samples)", type="primary"):
        try:
            with st.spinner("Scanning DB..."):
                prof = build_db_profile()
                st.session_state.db_profile = prof

                cache_dir = Path(os.getenv("CACHE_DIR", "./cache"))
                mem = MemoryStore(cache_dir)
                mem_json = mem.load_json()
                mem_json["db_profile"] = prof
                mem.save_json(mem_json)

            st.success(f"Scanned {len(prof.get('tables', []))} tables.")
        except Exception as e:
            st.error(f"DB scan failed: {e}")

    if st.session_state.db_profile.get("tables"):
        st.write("**Tables discovered:**")
        st.dataframe(
            pd.DataFrame(
                [
                    {"schema": t["schema"], "table": t["table"], "row_count": t["row_count"], "cols": len(t["columns"])}
                    for t in st.session_state.db_profile["tables"]
                ]
            ),
            width='stretch',
        )

with right:
    st.subheader("2) Dashboard Request")
    user_request = st.text_area(
        "What dashboard do you want? (Example: leadership dashboard: earnings, redemptions, active customers, trends, filters...)",
        height=140,
        key="user_request",
    )

    st.subheader("3) Step Controller")

    if not st.session_state.team_ready:
        if st.button("Initialize AI Team"):
            try:
                team, clients = build_team()
                st.session_state.team = team
                st.session_state.model_clients = clients
                st.session_state.team_ready = True
                st.success("Team initialized.")
            except Exception as e:
                st.error(f"Team init failed: {e}")

    step_idx = st.session_state.current_step_idx
    step_key, step_goal = STEPS[step_idx]
    st.write(f"**Current Step:** `{step_key}`")
    st.write(step_goal)

    can_run = st.session_state.team_ready and bool(st.session_state.db_profile.get("tables"))
    if not can_run:
        st.info ("To run steps: Initialize AI Team + Scan DB first.")

    run_clicked = st.button("Run This Step", disabled=not can_run)

    if run_clicked:
        task = step_task_prompt(step_key, step_goal, user_request, st.session_state.db_profile)
        with st.spinner(f"Running {step_key}... (streaming)"):
            try:
                asyncio.run(run_team_stream(st.session_state.team, task, step_key))
                st.success("Step completed (see outputs below).")
                st.rerun()
            except Exception as e:
                st.error(f"Step run failed: {e}")

    st.markdown("---")
    if st.button("✅ Approve & Move to Next Step"):
        st.session_state.approved_steps.add(step_key)
        if st.session_state.current_step_idx < len(STEPS) - 1:
            st.session_state.current_step_idx += 1
            st.rerun()
        else:
            st.success("All steps approved.")

st.markdown("---")
st.subheader("Step Outputs (streamed)")

tabs = st.tabs([k for k, _ in STEPS])
for i, (k, _) in enumerate(STEPS):
    with tabs[i]:
        render_logs(k)

#----------------------Table Description -----------------------------
st.markdown("## 📚 Data Catalog")

for t in st.session_state.db_profile.get("tables", []):
    with st.expander(f"📄 {t['schema']}.{t['table']}"):
        st.markdown(f"**Description**: {t.get('table_description', '—')}")
        st.markdown(f"**Business Meaning**: {t.get('business_meaning', '—')}")

        if t.get("important_columns"):
            st.markdown("**Important Columns**")
            st.write(", ".join(t["important_columns"]))

        if t.get("dashboard_use_cases"):
            st.markdown("**Dashboard Use Cases**")
            for uc in t["dashboard_use_cases"]:
                st.markdown(f"- {uc}")

        if t.get("typical_joins"):
            st.markdown("**Typical Joins**")
            for j in t["typical_joins"]:
                st.markdown(f"- {j}")
# ----------------- Task-4/5 execution -----------------
st.markdown("---")
st.subheader("🚀 Task-4/5 Execution (Real): SQL → Cache → KPIs → Feed Dashboard")

st.caption("This parses SQLBuilder JSON from TASK_4_INTERMEDIATE_VIEWS, executes SELECT-only SQL, caches results (Parquet/DuckDB), computes KPIs, and prepares real datasets for dashboard generation.")

exec_disabled = not (st.session_state.db_profile.get("tables") and st.session_state.team_ready)

if st.session_state.logs:
    agents_in_task4 = sorted({x["agent"] for x in st.session_state.logs if x["step"] == "TASK_4_INTERMEDIATE_VIEWS"})
    st.caption(f"Agents logged for TASK_4_INTERMEDIATE_VIEWS: {agents_in_task4}")

run_exec = st.button("▶ Execute Task-4 SQL + Compute KPIs (Task-5)", type="primary", disabled=exec_disabled)

if run_exec:
    sql_text = latest_agent_output("TASK_4_INTERMEDIATE_VIEWS")
    if not sql_text:
        st.error("No SQLBuilder output found for TASK_4_INTERMEDIATE_VIEWS. Run Task-4 first.")
    else:
        try:
            # 1) Parse Task-4 SQLBuilder JSON
            with st.spinner("Parsing SQLBuilder JSON into Pydantic schema..."):
                sql_out = parse_llm_json(sql_text, SQLBuildOutput)

            # 2) Parse Task-3 Planner JSON (optional but recommended)
            plan_text = latest_agent_output("TASK_3_ANALYSIS_PLAN", "planner")
            analysis_plan = None
            if plan_text:
                try:
                    plan_obj = parse_llm_json(plan_text, AnalysisPlan)
                    analysis_plan = plan_obj.model_dump()
                except Exception as e:
                    st.warning(f"Could not parse AnalysisPlan JSON from TASK_3; using generic KPIs. Error: {e}")

            # 3) Execute SQL, cache datasets, compute KPIs (generic + plan-driven)
            with st.spinner("Executing SQL safely, caching datasets, and computing KPIs..."):
                engine = build_engine()
                cache_dir = Path(os.getenv("CACHE_DIR", "./cache"))
                mem = MemoryStore(cache_dir)

                bundle, previews = execute_and_cache_artifacts(
                    engine,
                    mem,
                    sql_out.artifacts,
                    analysis_plan=analysis_plan,   # <-- KEY CHANGE
                )

                # Persist to memory.json
                mem_json = mem.load_json()
                mem_json["execution_bundle"] = bundle.model_dump()
                if analysis_plan:
                    mem_json["analysis_plan"] = analysis_plan
                mem.save_json(mem_json)

                # Store in session for UI + dashboard build
                st.session_state.execution_bundle = bundle.model_dump()
                st.session_state.dataset_previews = {k: v.to_dict(orient="records") for k, v in previews.items()}

            st.success("Execution complete ✅ Datasets cached + KPIs computed (generic + planner-driven).")

        except Exception as e:
            st.error(f"Execution failed: {e}")

if st.session_state.execution_bundle:
    st.markdown("### ✅ KPI Reports (computed)")
    st.json(st.session_state.execution_bundle)

if st.session_state.dataset_previews:
    st.markdown("### 👀 Dataset Previews (top rows)")
    for name, rows in st.session_state.dataset_previews.items():
        st.write(f"**{name}**")
        st.dataframe(pd.DataFrame(rows), width='stretch')

# ----------------- Build dashboard from cached datasets -----------------
st.markdown("---")
st.subheader("📊 Build Real Dashboard from Cached Data (Task-7)")

build_real = st.button("🧱 Generate Dashboard using Cached Datasets", disabled=(not st.session_state.execution_bundle))

if build_real:
    try:
        bundle = st.session_state.execution_bundle
        previews = st.session_state.dataset_previews or {}

        task = f"""
You must build a production HTML/CSS/JS dashboard using Plotly.
You MUST use the real datasets and KPI reports below.

EXECUTION_BUNDLE_JSON:
{safe_json_dumps(bundle, indent=2)[:60000]}

DATASET_PREVIEWS_JSON (top rows only):
{safe_json_dumps(previews, indent=2)[:60000]}

Requirements:
- KPI cards from KPI report.
- At least 4 charts:
  1) trend (if time col exists)
  2) category breakdown
  3) distribution of key numeric column
  4) a table view
- Filters: date range if available, 1-2 categorical filters.
- Responsive layout.
Return full HTML in one block starting with <html>.
"""
        with st.spinner("Generating real dashboard HTML..."):
            asyncio.run(run_team_stream(st.session_state.team, task, "TASK_7_DASHBOARD_BUILD"))
        st.success("Dashboard generated ✅ Scroll down to render.")
    except Exception as e:
        st.error(f"Dashboard build failed: {e}")

st.markdown("---")
st.subheader("Generated Dashboard (when available)")

latest_html = None
for it in reversed(st.session_state.logs):
    if it["agent"] == "dashboard_builder" and "<html" in it["content"].lower():
        latest_html = it["content"]
        break

if latest_html:
    st.session_state.dashboard_html = latest_html

if st.session_state.dashboard_html:
    st.components.v1.html(st.session_state.dashboard_html, height=900, scrolling=True)
else:
    st.info ("Dashboard HTML will appear after TASK_7_DASHBOARD_BUILD.")