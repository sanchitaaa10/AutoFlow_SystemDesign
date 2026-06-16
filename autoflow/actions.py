from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from autoflow.utils import get_path, render_template, resolve_parameter, resolve_parameters


ActionHandler = Callable[[dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]]


class ActionRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, ActionHandler] = {}

    def register(self, name: str, handler: ActionHandler) -> None:
        self._handlers[name] = handler

    def has(self, name: str) -> bool:
        return name in self._handlers

    def execute(
        self,
        action_name: str,
        context: dict[str, Any],
        parameters: dict[str, Any],
        services: dict[str, Any],
    ) -> dict[str, Any]:
        if action_name not in self._handlers:
            raise ValueError(f"No action registered for '{action_name}'")
        return self._handlers[action_name](context, parameters, services)


def validate_payload_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    required_fields = parameters.get("required_fields", [])
    missing = []
    for field in required_fields:
        path = field if str(field).startswith("payload.") else f"payload.{field}"
        if get_path(context, path) in (None, ""):
            missing.append(field)

    if missing:
        raise ValueError(f"Missing required payload fields: {', '.join(missing)}")

    return {
        "valid": True,
        "required_fields": required_fields,
        "checked_by": "validate_payload_action",
    }


def send_email_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    resolved = resolve_parameters(parameters, context)
    to_address = resolved.get("to")
    subject = resolved.get("subject", "AutoFlow Notification")
    body = resolved.get("body", "")

    if not to_address:
        raise ValueError("Email action requires a 'to' address")

    return {
        "sent": True,
        "provider": "mock-email-service",
        "to": to_address,
        "subject": subject,
        "body_preview": str(body)[:120],
    }


def update_database_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    database = services["database"]
    key = render_template(parameters.get("key", ""), context)
    if not key:
        raise ValueError("Database update action requires a 'key'")

    if "value_from" in parameters:
        value = get_path(context, parameters["value_from"])
    else:
        value = resolve_parameter(parameters.get("value", {}), context)

    database.upsert_user_config(key, value)
    return {"updated": True, "key": key, "value": value}


def call_external_api_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    resolved = resolve_parameters(parameters, context)
    endpoint = resolved.get("endpoint")
    if not endpoint:
        raise ValueError("External API action requires an 'endpoint'")
    if resolved.get("simulate_failure"):
        raise RuntimeError(f"Simulated external service failure for {endpoint}")

    request_payload = (
        get_path(context, parameters["payload_from"])
        if "payload_from" in parameters
        else resolved.get("payload", {})
    )

    return {
        "called": True,
        "simulated": True,
        "method": resolved.get("method", "POST"),
        "endpoint": endpoint,
        "status_code": 200,
        "request_payload": request_payload,
    }


def generate_report_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    title = render_template(parameters.get("title", "AutoFlow Execution Report"), context)
    report = {
        "title": title,
        "workflow_id": services.get("workflow_id"),
        "run_id": services.get("run_id"),
        "payload": deepcopy(context.get("payload", {})),
        "step_outputs": deepcopy(context.get("steps", {})),
    }

    output_path = parameters.get("output_path")
    if output_path:
        path = Path(render_template(output_path, context))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["file"] = str(path)

    return {"generated": True, "report": report}


def request_approval_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    amount_path = parameters.get("amount_path", "payload.amount")
    amount = float(get_path(context, amount_path, 0))
    threshold = float(parameters.get("auto_approve_until", 0))
    approver = parameters.get("approver", "manager@example.com")
    approved = amount <= threshold

    return {
        "approval_required": not approved,
        "approved": approved,
        "amount": amount,
        "threshold": threshold,
        "approver": approver,
    }


def transform_payload_action(
    context: dict[str, Any], parameters: dict[str, Any], services: dict[str, Any]
) -> dict[str, Any]:
    mapping = parameters.get("mapping", {})
    transformed = {
        output_key: resolve_parameter(source, context)
        for output_key, source in mapping.items()
    }
    return {"transformed": transformed}


def register_default_actions(registry: ActionRegistry) -> None:
    registry.register("validate_payload", validate_payload_action)
    registry.register("send_email", send_email_action)
    registry.register("update_database", update_database_action)
    registry.register("call_external_api", call_external_api_action)
    registry.register("generate_report", generate_report_action)
    registry.register("request_approval", request_approval_action)
    registry.register("transform_payload", transform_payload_action)
