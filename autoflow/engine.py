from __future__ import annotations

import threading
import time
from typing import Any

from autoflow.actions import ActionRegistry, register_default_actions
from autoflow.broker import InMemoryMessageBroker
from autoflow.database import AutoFlowDatabase
from autoflow.models import RunStatus, WorkflowDefinition, WorkflowEvent
from autoflow.monitoring import MonitoringService
from autoflow.orchestrator import WorkflowOrchestrator


class AutoFlowEngine:
    def __init__(self, database_path: str = "autoflow.db", worker_count: int = 2) -> None:
        self.database = AutoFlowDatabase(database_path)
        self.broker = InMemoryMessageBroker()
        self.actions = ActionRegistry()
        register_default_actions(self.actions)
        self.orchestrator = WorkflowOrchestrator(
            self.database,
            self.broker,
            self.actions,
        )
        self.monitoring = MonitoringService(self.database, self.broker)
        self.worker_count = worker_count
        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._workers or self.worker_count <= 0:
            return
        self._stop_event.clear()
        for index in range(self.worker_count):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"autoflow-worker-{index + 1}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def stop(self) -> None:
        self._stop_event.set()
        self.broker.cancel_timers()
        for worker in self._workers:
            worker.join(timeout=2)
        self._workers.clear()

    def close(self) -> None:
        self.stop()
        self.database.close()

    def register_workflow(
        self, workflow: WorkflowDefinition, actor: str = "system"
    ) -> str:
        return self.orchestrator.register_workflow(workflow, actor=actor)

    def trigger(
        self,
        event_name: str,
        payload: dict[str, Any],
        source: str = "api",
    ) -> list[str]:
        event = WorkflowEvent(name=event_name, payload=payload, source=source)
        return self.orchestrator.trigger_event(event)

    def wait_for_runs(
        self,
        run_ids: list[str],
        timeout_seconds: float = 10,
        poll_interval: float = 0.05,
    ) -> dict[str, dict[str, Any]]:
        deadline = time.monotonic() + timeout_seconds
        terminal = {
            RunStatus.COMPLETED.value,
            RunStatus.FAILED.value,
            RunStatus.CANCELLED.value,
        }

        while time.monotonic() < deadline:
            runs = {
                run_id: self.database.get_run(run_id)
                for run_id in run_ids
            }
            if all(run and run["status"] in terminal for run in runs.values()):
                return runs
            time.sleep(poll_interval)

        raise TimeoutError(f"Timed out waiting for workflow runs: {run_ids}")

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            task = self.broker.consume(timeout_seconds=0.2)
            if task is None:
                continue
            try:
                self.orchestrator.execute_task(task)
            finally:
                self.broker.task_done()

