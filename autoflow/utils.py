from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any


MISSING = object()
TEMPLATE_PATTERN = re.compile(r"\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}")


def to_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, default=str)


def from_json(data: str | None, default: Any = None) -> Any:
    if data in (None, ""):
        return default
    return json.loads(data)


def get_path(data: dict[str, Any], path: str, default: Any = None) -> Any:
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def render_template(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return TEMPLATE_PATTERN.sub(
            lambda match: str(get_path(context, match.group(1), "")), value
        )
    if isinstance(value, list):
        return [render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_template(item, context) for key, item in value.items()}
    return value


def resolve_parameter(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, dict) and "$path" in value:
        return get_path(context, str(value["$path"]))
    return render_template(value, context)


def resolve_parameters(values: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return {key: resolve_parameter(value, context) for key, value in values.items()}


def deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def evaluate_condition(condition: dict[str, Any] | None, context: dict[str, Any]) -> bool:
    if not condition:
        return True

    if "all" in condition:
        return all(evaluate_condition(item, context) for item in condition["all"])
    if "any" in condition:
        return any(evaluate_condition(item, context) for item in condition["any"])

    path = condition.get("path")
    operator = condition.get("operator", "eq")
    expected = condition.get("value")
    actual = get_path(context, path, MISSING) if path else MISSING

    if operator == "exists":
        return actual is not MISSING
    if actual is MISSING:
        return False
    if operator == "eq":
        return actual == expected
    if operator == "ne":
        return actual != expected
    if operator == "gt":
        return actual > expected
    if operator == "gte":
        return actual >= expected
    if operator == "lt":
        return actual < expected
    if operator == "lte":
        return actual <= expected
    if operator == "contains":
        return expected in actual

    raise ValueError(f"Unsupported condition operator: {operator}")


def evaluate_filters(filters: dict[str, Any] | None, context: dict[str, Any]) -> bool:
    if not filters:
        return True
    if "path" in filters or "all" in filters or "any" in filters:
        return evaluate_condition(filters, context)

    for path, expected in filters.items():
        full_path = path if path.startswith(("payload.", "event.")) else f"payload.{path}"
        if get_path(context, full_path, MISSING) != expected:
            return False
    return True

