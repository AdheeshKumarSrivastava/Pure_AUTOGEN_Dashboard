from __future__ import annotations

import json
from typing import Dict


def beautify_step(step_key: str, agent: str, content: str) -> str:
    """
    Convert raw agent output into executive-friendly markdown.
    """
    header = f"### 🤖 {agent.replace('_', ' ').title()}"
    divider = "\n---\n"

    if step_key == "TASK_1_DB_PROFILE":
        return _beautify_db_profile(agent, content, header, divider)

    if step_key == "TASK_2_USER_NEED":
        return _beautify_user_need(agent, content, header, divider)

    if step_key == "TASK_3_ANALYSIS_PLAN":
        return _beautify_analysis_plan(agent, content, header, divider)

    # Default fallback
    return f"{header}\n\n{content}"


# ------------------- Specific beautifiers -------------------

def _beautify_db_profile(agent: str, content: str, header: str, divider: str) -> str:
    bullets = []
    lines = content.splitlines()

    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        if ln.lower().startswith(("table", "-", "*")):
            bullets.append(f"- {ln.lstrip('-* ')}")

    body = "\n".join(bullets) if bullets else content

    return f"""
{header}

📊 **Database Profiling Summary**

This step analyzes the database structure to understand **what data is available and how it can be used**.

**Key Findings**
{body}

💡 *Outcome:* Database is now ready for analytical planning.

{divider}
"""


def _beautify_user_need(agent: str, content: str, header: str, divider: str) -> str:
    return f"""
{header}

🧭 **Understanding User Requirements**

{content}

✅ *Outcome:* Dashboard requirements clarified.

{divider}
"""


def _beautify_analysis_plan(agent: str, content: str, header: str, divider: str) -> str:
    return f"""
{header}

🧠 **Proposed Analytical Plan**

{content}

📈 *Outcome:* Clear roadmap for KPIs, charts, and datasets.

{divider}
"""