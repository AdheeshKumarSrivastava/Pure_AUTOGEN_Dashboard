from __future__ import annotations

import json
import re
from typing import Any, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


_THINK_RE = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", flags=re.DOTALL | re.IGNORECASE)


def _strip_think(text: str) -> str:
    return _THINK_RE.sub("", text or "").strip()


def _extract_json_candidate(text: str) -> str:
    """
    Try, in order:
    1) JSON inside ```json ... ```
    2) first {...} object
    3) first [...] array
    """
    t = _strip_think(text)

    # 1) fenced
    m = _CODE_FENCE_RE.search(t)
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate

    # 2) first object
    obj_start = t.find("{")
    obj_end = t.rfind("}")
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        return t[obj_start : obj_end + 1].strip()

    # 3) first array
    arr_start = t.find("[")
    arr_end = t.rfind("]")
    if arr_start != -1 and arr_end != -1 and arr_end > arr_start:
        return t[arr_start : arr_end + 1].strip()

    return t # fallback (will likely fail, but gives visibility)


def parse_llm_json(text: str, model: Type[T]) -> T:
    """
    Strict: returns a Pydantic model instance.
    Robust: strips <think>, extracts JSON block if present.
    """
    candidate = _extract_json_candidate(text)

    if not candidate or candidate.strip() == "":
        raise ValueError("Invalid JSON from model: empty response")

    try:
        data = json.loads(candidate)
    except Exception as e:
        # Helpful debug snippet
        snippet = candidate[:500].replace("\n", "\\n")
        raise ValueError(f"Invalid JSON from model: {e}. Candidate starts with: {snippet}") from e

    return model.model_validate(data)