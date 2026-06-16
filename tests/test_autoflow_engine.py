from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from autoflow import AutoFlowEngine, RetryPolicy, WorkflowDefinition, WorkflowStep


class AutoFlowEngineTests(unittest.TestCase):
    def create_engine(self) -> tuple[AutoFlowEngine, tempfile.TemporaryDirectory[str]]:
        temp_dir = tempfile.TemporaryDirectory()
        database_path = str(Path(temp_dir.name) / "test_autoflow.db")
        return AutoFlowEngine(database_path, worker_count=2), temp_dir

    def test_workflow_completes_and_persists_step_output(self) -> None:
        engine, temp_dir = self.create_engine()
        workflow = WorkflowDefinition(
            name="Signup Workflow",
            trigger={"event": "user.signup"},
            steps=[
                WorkflowStep(
                    id="validate",
                    name="Validate Signup",
                    action="validate_payload",
                    parameters={"required_fields": ["email"]},
                ),
                WorkflowStep(
                    id="welcome_email",
                    name="Send Welcome Email",
                    action="send_email",
                    depends_on=["validate"],
                    parameters={
                        "to": "{{ payload.email }}",
                        "subject": "Welcome {{ payload.name }}",
                        "body": "Thanks for signing up.",
                    },
                ),
            ],
        )

        try:
            engine.register_workflow(workflow)
            engine.start()
            run_ids = engine.trigger(
                "user.signup",
                {"email": "student@example.com", "name": "Student"},
            )
            results = engine.wait_for_runs(run_ids, timeout_seconds=5)
            run_id = run_ids[0]

            self.assertEqual(results[run_id]["status"], "completed")
            details = engine.monitoring.run_details(run_id)
            step_statuses = {step["step_id"]: step["status"] for step in details["steps"]}
            self.assertEqual(step_statuses["validate"], "completed")
            self.assertEqual(step_statuses["welcome_email"], "completed")
        finally:
            engine.close()
            temp_dir.cleanup()

    def test_failed_step_is_retried_before_success(self) -> None:
        engine, temp_dir = self.create_engine()
        calls = {"count": 0}

        def flaky_action(context, parameters, services):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("temporary outage")
            return {"ok": True, "attempt": calls["count"]}

        engine.actions.register("flaky_action", flaky_action)
        workflow = WorkflowDefinition(
            name="Retry Workflow",
            trigger={"event": "retry.test"},
            retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.01),
            steps=[
                WorkflowStep(
                    id="flaky",
                    name="Flaky Step",
                    action="flaky_action",
                )
            ],
        )

        try:
            engine.register_workflow(workflow)
            engine.start()
            run_ids = engine.trigger("retry.test", {"id": "A1"})
            results = engine.wait_for_runs(run_ids, timeout_seconds=5)
            run_id = run_ids[0]
            details = engine.monitoring.run_details(run_id)
            statuses = [step["status"] for step in details["steps"]]

            self.assertEqual(results[run_id]["status"], "completed")
            self.assertEqual(calls["count"], 2)
            self.assertIn("retrying", statuses)
            self.assertIn("completed", statuses)
        finally:
            engine.close()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()

