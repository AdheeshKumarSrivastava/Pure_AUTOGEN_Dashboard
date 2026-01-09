from __future__ import annotations
import json
from typing import Any, Dict

def build_table_description_prompt(table: Dict[str, Any]) -> str:
    schema = table.get("schema")
    name = table.get("table")
    cols = table.get("columns", [])[:80]
    samples = table.get("sample_rows", [])[:5]

    # IMPORTANT: keep prompt compact but informative
    return f"""
You are TableDescriberAgent.
Return ONLY valid JSON. No markdown. No commentary. No extra text.
Do NOT include <think> tags.
If unsure, still return best guess.

JSON schema EXACTLY:
{{
  "description": "string (1-3 lines)",
  "business_meaning": "string (1-3 lines)",
  "important_columns": ["col1", "col2", "..."],
  "typical_joins": [
    {{
      "to_table": "schema.table",
      "on": ["left_col = right_col"],
      "join_type": "left"
    }}
  ],
  "dashboard_use_cases": ["use case 1", "use case 2"]
}}

TABLE:
schema: {schema}
table: {name}

COLUMNS (name/type/nullability/len):
{json.dumps(cols, indent=2)[:5000]}

SAMPLE_ROWS:
{json.dumps(samples, indent=2)[:5000]}
""".strip()