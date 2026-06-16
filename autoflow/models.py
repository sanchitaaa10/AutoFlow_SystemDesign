from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


class WorkflowStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DRAFT = "draft"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.2

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "backoff_seconds": self.backoff_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "RetryPolicy":
        if not data:
            return cls()
        return cls(
            max_attempts=int(data.get("max_attempts", 3)),
            backoff_seconds=float(data.get("backoff_seconds", 0.2)),
        )


@dataclass(frozen=True)
class WorkflowStep:
    id: str
    name: str
    action: str
    parameters: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    condition: dict[str, Any] | None = None
    timeout_seconds: int = 30

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "action": self.action,
            "parameters": self.parameters,
            "depends_on": self.depends_on,
            "condition": self.condition,
            "timeout_seconds": self.timeout_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowStep":
        return cls(
            id=data["id"],
            name=data["name"],
            action=data["action"],
            parameters=data.get("parameters", {}),
            depends_on=list(data.get("depends_on", [])),
            condition=data.get("condition"),
            timeout_seconds=int(data.get("timeout_seconds", 30)),
        )


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    trigger: dict[str, Any]
    steps: list[WorkflowStep]
    id: str = field(default_factory=lambda: new_id("wf"))
    status: WorkflowStatus = WorkflowStatus.ACTIVE
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    created_by: str = "system"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "trigger": self.trigger,
            "steps": [step.to_dict() for step in self.steps],
            "status": self.status.value,
            "retry_policy": self.retry_policy.to_dict(),
            "created_by": self.created_by,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowDefinition":
        return cls(
            id=data["id"],
            name=data["name"],
            trigger=data["trigger"],
            steps=[WorkflowStep.from_dict(step) for step in data.get("steps", [])],
            status=WorkflowStatus(data.get("status", WorkflowStatus.ACTIVE.value)),
            retry_policy=RetryPolicy.from_dict(data.get("retry_policy")),
            created_by=data.get("created_by", "system"),
            metadata=data.get("metadata", {}),
        )


@dataclass(frozen=True)
class WorkflowEvent:
    name: str
    payload: dict[str, Any]
    source: str = "api"
    id: str = field(default_factory=lambda: new_id("evt"))
    created_at: str = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "source": self.source,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class TaskMessage:
    run_id: str
    workflow_id: str
    step_id: str
    step_run_id: str
    attempt: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "step_id": self.step_id,
            "step_run_id": self.step_run_id,
            "attempt": self.attempt,
        }

