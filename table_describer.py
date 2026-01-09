from __future__ import annotations
from typing import Dict, Any
import json


def build_table_description_prompt(table_profile: Dict[str, Any]) -> str:
    return f"""
You are a senior data architect.

Given the following database table metadata, generate:
1. A concise human-readable description
2. What business process it represents
3. Typical use cases in dashboards
4. Important columns to know
5. Common joins (if inferable)

TABLE_METADATA_JSON:
{json.dumps(table_profile, indent=2)}

Return STRICT JSON with keys:
- description
- business_meaning
- important_columns
- typical_joins
- dashboard_use_cases
"""