from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from autoflow.models import (
    LogLevel,
    RunStatus,
    StepStatus,
    WorkflowDefinition,
    WorkflowEvent,
    new_id,
    utc_now,
)
from autoflow.utils import from_json, to_json


class AutoFlowDatabase:
    def __init__(self, path: str = "autoflow.db") -> None:
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _init_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS workflow_definitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                trigger_name TEXT NOT NULL,
                definition_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS workflow_runs (
                id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                status TEXT NOT NULL,
                event_json TEXT NOT NULL,
                context_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error TEXT,
                FOREIGN KEY (workflow_id) REFERENCES workflow_definitions(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS step_runs (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                input_json TEXT,
                output_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                FOREIGN KEY (run_id) REFERENCES workflow_runs(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS event_logs (
                id TEXT PRIMARY KEY,
                run_id TEXT,
                workflow_id TEXT,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                metadata_json TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_trails (
                id TEXT PRIMARY KEY,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                details_json TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_configurations (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dead_letter_events (
                id TEXT PRIMARY KEY,
                run_id TEXT,
                workflow_id TEXT,
                event_json TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_steps_run ON step_runs(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_logs_run ON event_logs(run_id)",
        ]
        with self._lock:
            for statement in statements:
                self._connection.execute(statement)
            self._connection.commit()

    def save_workflow(self, workflow: WorkflowDefinition) -> None:
        now = utc_now()
        trigger_name = workflow.trigger.get("event", "*")
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO workflow_definitions (
                    id, name, trigger_name, definition_json, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    trigger_name = excluded.trigger_name,
                    definition_json = excluded.definition_json,
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                (
                    workflow.id,
                    workflow.name,
                    trigger_name,
                    to_json(workflow.to_dict()),
                    workflow.status.value,
                    now,
                    now,
                ),
            )
            self._connection.commit()

    def get_workflow(self, workflow_id: str) -> WorkflowDefinition | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT definition_json FROM workflow_definitions WHERE id = ?",
                (workflow_id,),
            ).fetchone()
        if not row:
            return None
        return WorkflowDefinition.from_dict(from_json(row["definition_json"]))

    def list_workflows(self, active_only: bool = True) -> list[WorkflowDefinition]:
        query = "SELECT definition_json FROM workflow_definitions"
        params: tuple[Any, ...] = ()
        if active_only:
            query += " WHERE status = ?"
            params = ("active",)
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [WorkflowDefinition.from_dict(from_json(row["definition_json"])) for row in rows]

    def create_run(
        self, workflow: WorkflowDefinition, event: WorkflowEvent, context: dict[str, Any]
    ) -> str:
        run_id = new_id("run")
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO workflow_runs (
                    id, workflow_id, status, event_json, context_json, started_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    workflow.id,
                    RunStatus.PENDING.value,
                    to_json(event.to_dict()),
                    to_json(context),
                    utc_now(),
                ),
            )
            self._connection.commit()
        return run_id

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM workflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["event"] = from_json(data.pop("event_json"), {})
        data["context"] = from_json(data.pop("context_json"), {})
        return data

    def update_run_status(
        self,
        run_id: str,
        status: RunStatus,
        error: str | None = None,
        finished: bool = False,
    ) -> None:
        finished_at = utc_now() if finished else None
        with self._lock:
            self._connection.execute(
                """
                UPDATE workflow_runs
                SET status = ?, error = COALESCE(?, error),
                    finished_at = COALESCE(?, finished_at)
                WHERE id = ?
                """,
                (status.value, error, finished_at, run_id),
            )
            self._connection.commit()

    def update_run_context(self, run_id: str, context: dict[str, Any]) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE workflow_runs SET context_json = ? WHERE id = ?",
                (to_json(context), run_id),
            )
            self._connection.commit()

    def create_step_run(
        self,
        run_id: str,
        workflow_id: str,
        step_id: str,
        attempt: int,
        status: StepStatus = StepStatus.PENDING,
    ) -> str:
        step_run_id = new_id("step")
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO step_runs (
                    id, run_id, workflow_id, step_id, status, attempt, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    step_run_id,
                    run_id,
                    workflow_id,
                    step_id,
                    status.value,
                    attempt,
                    utc_now(),
                ),
            )
            self._connection.commit()
        return step_run_id

    def get_step_run(self, step_run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM step_runs WHERE id = ?", (step_run_id,)
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["input"] = from_json(data.pop("input_json"), {})
        data["output"] = from_json(data.pop("output_json"), {})
        return data

    def get_step_runs(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM step_runs
                WHERE run_id = ?
                ORDER BY created_at ASC, attempt ASC
                """,
                (run_id,),
            ).fetchall()
        runs = []
        for row in rows:
            data = dict(row)
            data["input"] = from_json(data.pop("input_json"), {})
            data["output"] = from_json(data.pop("output_json"), {})
            runs.append(data)
        return runs

    def get_step_statuses(self, run_id: str) -> dict[str, StepStatus]:
        statuses: dict[str, StepStatus] = {}
        for step_run in self.get_step_runs(run_id):
            statuses[step_run["step_id"]] = StepStatus(step_run["status"])
        return statuses

    def update_step_run(
        self,
        step_run_id: str,
        status: StepStatus,
        input_data: dict[str, Any] | None = None,
        output_data: dict[str, Any] | None = None,
        error: str | None = None,
        finished: bool = False,
    ) -> None:
        started_at = utc_now() if status == StepStatus.RUNNING else None
        finished_at = utc_now() if finished else None
        with self._lock:
            self._connection.execute(
                """
                UPDATE step_runs
                SET status = ?,
                    input_json = COALESCE(?, input_json),
                    output_json = COALESCE(?, output_json),
                    error = COALESCE(?, error),
                    started_at = COALESCE(?, started_at),
                    finished_at = COALESCE(?, finished_at)
                WHERE id = ?
                """,
                (
                    status.value,
                    to_json(input_data) if input_data is not None else None,
                    to_json(output_data) if output_data is not None else None,
                    error,
                    started_at,
                    finished_at,
                    step_run_id,
                ),
            )
            self._connection.commit()

    def log_event(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        run_id: str | None = None,
        workflow_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO event_logs (
                    id, run_id, workflow_id, level, message, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_id("log"),
                    run_id,
                    workflow_id,
                    level.value,
                    message,
                    to_json(metadata or {}),
                    utc_now(),
                ),
            )
            self._connection.commit()

    def audit(
        self,
        actor: str,
        action: str,
        entity_type: str,
        entity_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO audit_trails (
                    id, actor, action, entity_type, entity_id, details_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_id("audit"),
                    actor,
                    action,
                    entity_type,
                    entity_id,
                    to_json(details or {}),
                    utc_now(),
                ),
            )
            self._connection.commit()

    def add_dead_letter(
        self,
        run_id: str,
        workflow_id: str,
        event: dict[str, Any],
        reason: str,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO dead_letter_events (
                    id, run_id, workflow_id, event_json, reason, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    new_id("dead"),
                    run_id,
                    workflow_id,
                    to_json(event),
                    reason,
                    utc_now(),
                ),
            )
            self._connection.commit()

    def upsert_user_config(self, key: str, value: Any) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO user_configurations (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, to_json(value), utc_now()),
            )
            self._connection.commit()

    def get_user_config(self, key: str) -> Any:
        with self._lock:
            row = self._connection.execute(
                "SELECT value_json FROM user_configurations WHERE key = ?", (key,)
            ).fetchone()
        if not row:
            return None
        return from_json(row["value_json"])

    def counts_by_status(self, table: str) -> dict[str, int]:
        if table not in {"workflow_runs", "step_runs"}:
            raise ValueError("Unsupported table for status counts")
        with self._lock:
            rows = self._connection.execute(
                f"SELECT status, COUNT(*) AS total FROM {table} GROUP BY status"
            ).fetchall()
        return {row["status"]: int(row["total"]) for row in rows}

    def recent_logs(self, limit: int = 10) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM event_logs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        logs = []
        for row in rows:
            data = dict(row)
            data["metadata"] = from_json(data.pop("metadata_json"), {})
            logs.append(data)
        return logs

