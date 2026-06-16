from __future__ import annotations

import queue
import threading

from autoflow.models import TaskMessage


class InMemoryMessageBroker:
    def __init__(self) -> None:
        self._queue: queue.Queue[TaskMessage] = queue.Queue()
        self._timers: list[threading.Timer] = []
        self._lock = threading.Lock()

    def publish(self, task: TaskMessage, delay_seconds: float = 0.0) -> None:
        if delay_seconds <= 0:
            self._queue.put(task)
            return

        timer = threading.Timer(delay_seconds, lambda: self._queue.put(task))
        timer.daemon = True
        with self._lock:
            self._timers.append(timer)
        timer.start()

    def consume(self, timeout_seconds: float = 0.2) -> TaskMessage | None:
        try:
            return self._queue.get(timeout=timeout_seconds)
        except queue.Empty:
            return None

    def task_done(self) -> None:
        self._queue.task_done()

    def depth(self) -> int:
        return self._queue.qsize()

    def cancel_timers(self) -> None:
        with self._lock:
            for timer in self._timers:
                timer.cancel()
            self._timers.clear()

