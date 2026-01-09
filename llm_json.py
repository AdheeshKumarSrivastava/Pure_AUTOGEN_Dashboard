from __future__ import annotations

import json
import re
from typing import Any, Type, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


def extract_json_block(text: str) -> str:
    """
    Tries to pull a JSON object from:
    - ```json ... ```
    - { ... } first JSON object
    """
    # fenced block
    m = re.search(r"```json\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # any object block from first { to last }
    m2 = re.search(r"(\{[\s\S]*\})", text)
    if m2:
        return m2.group(1).strip()

    raise ValueError("No JSON found in model output.")


def parse_llm_json(text: str, model: Type[T]) -> T:
    raw = extract_json_block(text)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from model: {e}") from e

    try:
        return model.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"JSON does not match expected schema: {e}") from e