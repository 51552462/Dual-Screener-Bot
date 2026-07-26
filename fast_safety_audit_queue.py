"""Bounded, non-blocking, in-memory Fast Safety audit emitter."""

from __future__ import annotations

from collections.abc import Mapping
from queue import Empty, Full, Queue
from types import MappingProxyType


class BoundedAuditEmitter:
    """Thread-safe bounded audit queue with best-effort emission."""

    def __init__(self, maxsize: int = 1024) -> None:
        if isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize < 1:
            raise ValueError("maxsize must be an integer greater than or equal to 1")

        self.__queue = Queue(maxsize=maxsize)

    def try_emit(self, event) -> bool:
        """Copy and enqueue a mapping without blocking the caller."""
        if not isinstance(event, Mapping):
            return False

        try:
            snapshot = MappingProxyType(dict(event))
            self.__queue.put_nowait(snapshot)
            return True
        except Full:
            return False
        except Exception:
            return False

    def try_get(self):
        """Return the next event immediately, or None when unavailable."""
        try:
            return self.__queue.get_nowait()
        except Empty:
            return None
        except Exception:
            return None

    def drain(self, limit=None) -> tuple:
        """Return currently available events in FIFO order without blocking."""
        if limit is not None:
            if (
                isinstance(limit, bool)
                or not isinstance(limit, int)
                or limit < 0
            ):
                raise ValueError(
                    "limit must be None or an integer greater than or equal to 0"
                )

        target = self.qsize() if limit is None else limit
        events = []

        for _ in range(target):
            event = self.try_get()
            if event is None:
                break
            events.append(event)

        return tuple(events)

    def qsize(self) -> int:
        """Return the queue's approximate current size."""
        return self.__queue.qsize()
