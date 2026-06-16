from __future__ import annotations

from typing import Any

from autoflow.broker import InMemoryMessageBroker
from autoflow.database import AutoFlowDatabase


class MonitoringService:
    def __init__(
        self, database: AutoFlowDatabase, broker: InMemoryMessageBroker
    ) -> None:
        self.database = database
        self.broker = broker

    def dashboard(self) -> dict[str, Any]:
        return {
            "workflow_runs": self.database.counts_by_status("workflow_runs"),
            "step_runs": self.database.counts_by_status("step_runs"),
            "queue_depth": self.broker.depth(),
            "recent_logs": self.database.recent_logs(limit=10),
        }

    def run_details(self, run_id: str) -> dict[str, Any]:
        return {
            "run": self.database.get_run(run_id),
            "steps": self.database.get_step_runs(run_id),
            "logs": [
                log
                for log in self.database.recent_logs(limit=100)
                if log.get("run_id") == run_id
            ],
        }

