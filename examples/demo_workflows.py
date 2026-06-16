from __future__ import annotations

import json
from pathlib import Path

from autoflow import AutoFlowEngine, RetryPolicy, WorkflowDefinition, WorkflowStep


def build_order_processing_workflow() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="Order Processing Workflow",
        trigger={"event": "order.created"},
        retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=0.1),
        metadata={"owner": "operations-team"},
        steps=[
            WorkflowStep(
                id="validate_order",
                name="Validate Order Payload",
                action="validate_payload",
                parameters={
                    "required_fields": [
                        "order_id",
                        "customer_email",
                        "customer_name",
                        "total",
                    ]
                },
            ),
            WorkflowStep(
                id="send_confirmation",
                name="Send Customer Confirmation",
                action="send_email",
                depends_on=["validate_order"],
                parameters={
                    "to": "{{ payload.customer_email }}",
                    "subject": "Order {{ payload.order_id }} received",
                    "body": (
                        "Hello {{ payload.customer_name }}, your order "
                        "{{ payload.order_id }} worth Rs. {{ payload.total }} "
                        "has been received."
                    ),
                },
            ),
            WorkflowStep(
                id="update_order_store",
                name="Update Order Store",
                action="update_database",
                depends_on=["validate_order"],
                parameters={
                    "key": "order:{{ payload.order_id }}",
                    "value_from": "payload",
                },
            ),
            WorkflowStep(
                id="sync_crm",
                name="Sync CRM System",
                action="call_external_api",
                depends_on=["validate_order"],
                parameters={
                    "endpoint": "https://crm.example.local/orders",
                    "method": "POST",
                    "payload_from": "payload",
                },
            ),
            WorkflowStep(
                id="generate_summary",
                name="Generate Execution Report",
                action="generate_report",
                depends_on=[
                    "send_confirmation",
                    "update_order_store",
                    "sync_crm",
                ],
                parameters={
                    "title": "Order {{ payload.order_id }} Execution Report",
                    "output_path": "reports/{{ event.id }}.json",
                },
            ),
        ],
    )


def build_high_value_approval_workflow() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="High Value Order Approval Workflow",
        trigger={
            "event": "order.created",
            "filters": {
                "path": "payload.total",
                "operator": "gte",
                "value": 1000,
            },
        },
        retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.1),
        steps=[
            WorkflowStep(
                id="request_manager_approval",
                name="Request Manager Approval",
                action="request_approval",
                parameters={
                    "amount_path": "payload.total",
                    "auto_approve_until": 5000,
                    "approver": "manager@autoflow.local",
                },
            ),
            WorkflowStep(
                id="notify_finance",
                name="Notify Finance Team",
                action="send_email",
                depends_on=["request_manager_approval"],
                parameters={
                    "to": "finance@autoflow.local",
                    "subject": "High value order {{ payload.order_id }}",
                    "body": (
                        "Order {{ payload.order_id }} requires finance visibility. "
                        "Current amount is Rs. {{ payload.total }}."
                    ),
                },
            ),
        ],
    )


def run_demo(database_path: str = "autoflow_demo.db") -> None:
    database_file = Path(database_path)
    if database_file.exists():
        database_file.unlink()

    engine = AutoFlowEngine(database_path, worker_count=3)
    engine.register_workflow(build_order_processing_workflow(), actor="admin")
    engine.register_workflow(build_high_value_approval_workflow(), actor="admin")

    try:
        engine.start()
        run_ids = engine.trigger(
            "order.created",
            {
                "order_id": "ORD-1001",
                "customer_name": "Sanchita",
                "customer_email": "sanchita@example.com",
                "total": 1299,
                "items": [
                    {"sku": "BOOK-101", "quantity": 1},
                    {"sku": "BAG-220", "quantity": 1},
                ],
            },
            source="demo-script",
        )
        results = engine.wait_for_runs(run_ids, timeout_seconds=8)

        print("Triggered workflow runs:")
        print(json.dumps(results, indent=2))
        print("\nMonitoring dashboard:")
        print(json.dumps(engine.monitoring.dashboard(), indent=2))
    finally:
        engine.close()


if __name__ == "__main__":
    run_demo()

