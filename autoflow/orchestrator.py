from __future__ import annotations

import threading
from typing import Any

from autoflow.actions import ActionRegistry
from autoflow.broker import InMemoryMessageBroker
from autoflow.database import AutoFlowDatabase
from autoflow.models import (
    LogLevel,
    RunStatus,
    StepStatus,
    TaskMessage,
    WorkflowDefinition,
    WorkflowEvent,
    WorkflowStep,
)
from autoflow.utils import deep_merge, evaluate_condition, evaluate_filters


class WorkflowValidationError(ValueError):
    pass


class WorkflowOrchestrator:
    SUCCESSFUL_STEP_STATES = {StepStatus.COMPLETED, StepStatus.SKIPPED}

    def __init__(
        self,
        database: AutoFlowDatabase,
        broker: InMemoryMessageBroker,
        actions: ActionRegistry,
    ) -> None:
        self.database = database
        self.broker = broker
        self.actions = actions
        self._lock = threading.RLock()

    def register_workflow(
        self, workflow: WorkflowDefinition, actor: str = "system"
    ) -> str:
        self.validate_workflow(workflow)
        self.database.save_workflow(workflow)
        self.database.audit(
            actor=actor,
            action="workflow_registered",
            entity_type="workflow",
            entity_id=workflow.id,
            details={"name": workflow.name, "trigger": workflow.trigger},
        )
        return workflow.id

    def validate_workflow(self, workflow: WorkflowDefinition) -> None:
        if not workflow.steps:
            raise WorkflowValidationError("A workflow must contain at least one step")

        step_ids = [step.id for step in workflow.steps]
        if len(step_ids) != len(set(step_ids)):
            raise WorkflowValidationError("Workflow step ids must be unique")

        known_steps = set(step_ids)
        for step in workflow.steps:
            if not self.actions.has(step.action):
                raise WorkflowValidationError(
                    f"Step '{step.id}' uses unknown action '{step.action}'"
                )
            for dependency in step.depends_on:
                if dependency not in known_steps:
                    raise WorkflowValidationError(
                        f"Step '{step.id}' depends on unknown step '{dependency}'"
                    )

        self._validate_no_cycles(workflow.steps)

    def trigger_event(self, event: WorkflowEvent) -> list[str]:
        matched_runs: list[str] = []
        for workflow in self.database.list_workflows(active_only=True):
            if not self._matches_trigger(workflow, event):
                continue

            context = {
                "event": event.to_dict(),
                "payload": event.payload,
                "vars": {},
                "steps": {},
            }
            run_id = self.database.create_run(workflow, event, context)
            matched_runs.append(run_id)
            self.database.update_run_status(run_id, RunStatus.RUNNING)
            self.database.log_event(
                message=f"Workflow '{workflow.name}' started",
                run_id=run_id,
                workflow_id=workflow.id,
                metadata={"event": event.name},
            )
            self._schedule_ready_steps(workflow, run_id)

        if not matched_runs:
            self.database.log_event(
                message=f"No active workflow matched event '{event.name}'",
                level=LogLevel.WARNING,
                metadata={"event": event.to_dict()},
            )

        return matched_runs

    def execute_task(self, task: TaskMessage) -> None:
        workflow = self.database.get_workflow(task.workflow_id)
        if not workflow:
            return

        step = self._find_step(workflow, task.step_id)
        if not step:
            return

        step_run = self.database.get_step_run(task.step_run_id)
        if not step_run or step_run["status"] != StepStatus.PENDING.value:
            return

        run = self.database.get_run(task.run_id)
        if not run or run["status"] != RunStatus.RUNNING.value:
            return

        context = run["context"]
        input_snapshot = {
            "payload": context.get("payload", {}),
            "previous_steps": context.get("steps", {}),
            "parameters": step.parameters,
        }
        self.database.update_step_run(
            task.step_run_id,
            StepStatus.RUNNING,
            input_data=input_snapshot,
        )

        try:
            output = self.actions.execute(
                step.action,
                context,
                step.parameters,
                {
                    "database": self.database,
                    "workflow_id": workflow.id,
                    "run_id": task.run_id,
                    "step_id": step.id,
                },
            )
            context.setdefault("steps", {})[step.id] = output
            if isinstance(output, dict) and isinstance(output.get("context_updates"), dict):
                context = deep_merge(context, output["context_updates"])

            self.database.update_run_context(task.run_id, context)
            self.database.update_step_run(
                task.step_run_id,
                StepStatus.COMPLETED,
                output_data=output,
                finished=True,
            )
            self.database.log_event(
                message=f"Step '{step.name}' completed",
                run_id=task.run_id,
                workflow_id=workflow.id,
                metadata={"step_id": step.id, "attempt": task.attempt},
            )
            self._schedule_ready_steps(workflow, task.run_id)
        except Exception as exc:
            self._handle_step_failure(workflow, step, task, exc)

    def _handle_step_failure(
        self,
        workflow: WorkflowDefinition,
        step: WorkflowStep,
        task: TaskMessage,
        exc: Exception,
    ) -> None:
        error_message = str(exc)
        retry_policy = workflow.retry_policy

        if task.attempt < retry_policy.max_attempts:
            self.database.update_step_run(
                task.step_run_id,
                StepStatus.RETRYING,
                error=error_message,
                finished=True,
            )
            next_attempt = task.attempt + 1
            next_step_run_id = self.database.create_step_run(
                task.run_id,
                workflow.id,
                step.id,
                attempt=next_attempt,
                status=StepStatus.PENDING,
            )
            self.broker.publish(
                TaskMessage(
                    run_id=task.run_id,
                    workflow_id=workflow.id,
                    step_id=step.id,
                    step_run_id=next_step_run_id,
                    attempt=next_attempt,
                ),
                delay_seconds=retry_policy.backoff_seconds * task.attempt,
            )
            self.database.log_event(
                message=f"Step '{step.name}' failed and will be retried",
                level=LogLevel.WARNING,
                run_id=task.run_id,
                workflow_id=workflow.id,
                metadata={
                    "step_id": step.id,
                    "attempt": task.attempt,
                    "error": error_message,
                },
            )
            return

        self.database.update_step_run(
            task.step_run_id,
            StepStatus.FAILED,
            error=error_message,
            finished=True,
        )
        self.database.update_run_status(
            task.run_id,
            RunStatus.FAILED,
            error=error_message,
            finished=True,
        )
        run = self.database.get_run(task.run_id) or {}
        self.database.add_dead_letter(
            run_id=task.run_id,
            workflow_id=workflow.id,
            event=run.get("event", {}),
            reason=error_message,
        )
        self.database.log_event(
            message=f"Workflow '{workflow.name}' failed at step '{step.name}'",
            level=LogLevel.ERROR,
            run_id=task.run_id,
            workflow_id=workflow.id,
            metadata={"step_id": step.id, "error": error_message},
        )

    def _schedule_ready_steps(self, workflow: WorkflowDefinition, run_id: str) -> None:
        with self._lock:
            made_progress = True
            while made_progress:
                made_progress = False
                run = self.database.get_run(run_id)
                if not run or run["status"] != RunStatus.RUNNING.value:
                    return

                context = run["context"]
                statuses = self.database.get_step_statuses(run_id)
                for step in workflow.steps:
                    if step.id in statuses:
                        continue
                    if not self._dependencies_satisfied(step, statuses):
                        continue

                    if not evaluate_condition(step.condition, context):
                        step_run_id = self.database.create_step_run(
                            run_id,
                            workflow.id,
                            step.id,
                            attempt=1,
                            status=StepStatus.SKIPPED,
                        )
                        self.database.update_step_run(
                            step_run_id,
                            StepStatus.SKIPPED,
                            output_data={"reason": "condition_not_met"},
                            finished=True,
                        )
                        self.database.log_event(
                            message=f"Step '{step.name}' skipped",
                            run_id=run_id,
                            workflow_id=workflow.id,
                            metadata={"step_id": step.id},
                        )
                        made_progress = True
                        break

                    step_run_id = self.database.create_step_run(
                        run_id,
                        workflow.id,
                        step.id,
                        attempt=1,
                        status=StepStatus.PENDING,
                    )
                    self.broker.publish(
                        TaskMessage(
                            run_id=run_id,
                            workflow_id=workflow.id,
                            step_id=step.id,
                            step_run_id=step_run_id,
                            attempt=1,
                        )
                    )
                    self.database.log_event(
                        message=f"Step '{step.name}' queued",
                        run_id=run_id,
                        workflow_id=workflow.id,
                        metadata={"step_id": step.id},
                    )
                    made_progress = True

            self._complete_run_if_finished(workflow, run_id)

    def _complete_run_if_finished(
        self, workflow: WorkflowDefinition, run_id: str
    ) -> None:
        statuses = self.database.get_step_statuses(run_id)
        if len(statuses) != len(workflow.steps):
            return
        if all(status in self.SUCCESSFUL_STEP_STATES for status in statuses.values()):
            self.database.update_run_status(run_id, RunStatus.COMPLETED, finished=True)
            self.database.log_event(
                message=f"Workflow '{workflow.name}' completed",
                run_id=run_id,
                workflow_id=workflow.id,
            )

    def _dependencies_satisfied(
        self, step: WorkflowStep, statuses: dict[str, StepStatus]
    ) -> bool:
        return all(
            statuses.get(dependency) in self.SUCCESSFUL_STEP_STATES
            for dependency in step.depends_on
        )

    def _matches_trigger(
        self, workflow: WorkflowDefinition, event: WorkflowEvent
    ) -> bool:
        trigger = workflow.trigger
        trigger_event = trigger.get("event")
        if trigger_event not in {event.name, "*"}:
            return False
        context = {"event": event.to_dict(), "payload": event.payload}
        return evaluate_filters(trigger.get("filters"), context)

    def _find_step(
        self, workflow: WorkflowDefinition, step_id: str
    ) -> WorkflowStep | None:
        for step in workflow.steps:
            if step.id == step_id:
                return step
        return None

    def _validate_no_cycles(self, steps: list[WorkflowStep]) -> None:
        graph = {step.id: set(step.depends_on) for step in steps}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(step_id: str) -> None:
            if step_id in visited:
                return
            if step_id in visiting:
                raise WorkflowValidationError("Workflow dependencies contain a cycle")
            visiting.add(step_id)
            for dependency in graph[step_id]:
                visit(dependency)
            visiting.remove(step_id)
            visited.add(step_id)

        for step_id in graph:
            visit(step_id)

