"""In-process async pub/sub event bus (§3.2.1).

The only coupling mechanism between modules — modules publish/subscribe on canonical topics rather
than importing each other (which keeps the R1 tier separation enforceable, §2.3). Single process, so
this is plain in-memory dispatch; no broker, no distributed state (E4).

Canonical topics (§3.2.1):
    tick · bar.1m · order.update · position.update · signal.candidate · proposal.created ·
    verdict.issued · risk.state · mode.changed · budget.state · feed.health
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from pydantic import BaseModel

from engine.core.log import get_logger

_log = get_logger("engine.core.eventbus")

Handler = Callable[[BaseModel], Awaitable[None]]


class EventBus:
    """Topic-keyed async pub/sub.

    ``publish`` is fire-and-forget (schedules handler tasks on the running loop); ``apublish`` awaits
    all handlers (deterministic ordering — used where a publisher needs delivery before proceeding, and
    in tests). A handler that raises is logged and isolated; it never breaks the publisher or sibling
    handlers (R8 observability over silent loss).
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Handler]] = {}

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subscribers.setdefault(topic, []).append(handler)

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        handlers = self._subscribers.get(topic)
        if handlers and handler in handlers:
            handlers.remove(handler)

    def publish(self, topic: str, event: BaseModel) -> None:
        """Fire-and-forget: schedule each handler as a task on the running loop."""
        handlers = self._subscribers.get(topic, ())
        if not handlers:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop (sync context / test). Deliver synchronously so events are never lost.
            asyncio.run(self._deliver_all(topic, list(handlers), event))
            return
        for handler in list(handlers):
            loop.create_task(self._deliver_one(topic, handler, event))

    async def apublish(self, topic: str, event: BaseModel) -> None:
        """Await delivery to every handler (exceptions isolated + logged)."""
        handlers = list(self._subscribers.get(topic, ()))
        await self._deliver_all(topic, handlers, event)

    async def _deliver_all(self, topic: str, handlers: list[Handler], event: BaseModel) -> None:
        await asyncio.gather(*(self._deliver_one(topic, h, event) for h in handlers))

    async def _deliver_one(self, topic: str, handler: Handler, event: BaseModel) -> None:
        try:
            await handler(event)
        except Exception:  # noqa: BLE001 - isolate a bad handler; never break the bus
            _log.exception("event_handler_failed", topic=topic, handler=getattr(handler, "__qualname__", str(handler)))
