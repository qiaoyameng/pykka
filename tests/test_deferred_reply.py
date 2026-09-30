from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest

from pykka import (
    Actor,
    ActorDeadError,
    CancelledError,
    DeferredReplyHandle,
    Timeout,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pykka import ActorRef
    from tests.types import Runtime


pytestmark = pytest.mark.usefixtures("_stop_all")


class Query(NamedTuple):
    pass


class Update(NamedTuple):
    value: str


class Fail(NamedTuple):
    message: str


class Precompleted(NamedTuple):
    pass


class Cancel(NamedTuple):
    pass


class EventualActor(Actor):
    def __init__(self, cancelled: Any = None) -> None:
        super().__init__()
        self.cancelled = cancelled
        self.reply: DeferredReplyHandle[str] | None = None
        self.value: str | None = None

    def on_receive(self, message: Any) -> Any:
        if isinstance(message, Query):
            if self.value is not None:
                return self.value
            self.reply = DeferredReplyHandle(on_cancel=self._cancel)
            return self.reply

        if isinstance(message, Update):
            self.value = message.value
            if self.reply is not None:
                self.reply.set(message.value)
                self.reply = None
            return None

        if isinstance(message, Fail):
            assert self.reply is not None
            self.reply.set_exception(ValueError(message.message))
            self.reply = None
            return None

        if isinstance(message, Precompleted):
            reply = DeferredReplyHandle[str]()
            reply.set("already ready")
            return reply

        if isinstance(message, Cancel):
            assert self.reply is not None
            self.reply.cancel()
            self.reply = None
            return None

        return super().on_receive(message)

    def _cancel(self) -> None:
        if self.cancelled is not None:
            self.cancelled.set()


@pytest.fixture
def actor_ref(runtime: Runtime) -> Iterator[ActorRef[EventualActor]]:
    class EventualActorImpl(
        EventualActor,
        runtime.actor_class,  # type: ignore[name-defined]
    ):
        pass

    ref = EventualActorImpl.start()
    yield ref
    ref.stop()


def test_query_does_not_block_update_that_completes_it(
    actor_ref: ActorRef[EventualActor],
) -> None:
    result: queue.Queue[str] = queue.Queue()

    def ask() -> None:
        result.put(actor_ref.ask(Query(), timeout=1))

    thread = threading.Thread(target=ask)
    thread.start()
    actor_ref.tell(Update("done"))

    thread.join(1)
    assert not thread.is_alive()
    assert result.get(timeout=1) == "done"


def test_deferred_reply_can_be_completed_with_exception(
    actor_ref: ActorRef[EventualActor],
) -> None:
    reply = actor_ref.ask(Query(), block=False)
    actor_ref.tell(Fail("broken"))

    with pytest.raises(ValueError, match="broken"):
        reply.get(timeout=1)


def test_deferred_reply_can_be_completed_before_it_is_bound(
    actor_ref: ActorRef[EventualActor],
) -> None:
    reply = actor_ref.ask(Precompleted(), block=False)

    assert reply.get(timeout=1) == "already ready"


def test_deferred_reply_can_only_be_completed_once() -> None:
    deferred = DeferredReplyHandle[str]()

    deferred.set("first")

    assert not deferred.cancel()

    with pytest.raises(queue.Full):
        deferred.set("second")


def test_cancel_completes_reply_and_calls_cancellation_callback(
    runtime: Runtime,
) -> None:
    cancelled = runtime.event_class()

    class CallbackActorImpl(
        EventualActor,
        runtime.actor_class,  # type: ignore[name-defined]
    ):
        pass

    actor_ref = CallbackActorImpl.start(cancelled)
    reply = actor_ref.ask(Query(), block=False)
    actor_ref.tell(Cancel())

    assert cancelled.wait(1)
    assert cancelled.is_set()
    with pytest.raises(CancelledError):
        reply.get(timeout=1)

    actor_ref.stop()


def test_caller_timeout_does_not_cancel_business_operation(
    actor_ref: ActorRef[EventualActor],
) -> None:
    reply = actor_ref.ask(Query(), block=False)

    with pytest.raises(Timeout):
        reply.get(timeout=0)

    actor_ref.tell(Update("late"))

    assert reply.get(timeout=1) == "late"


def test_stopping_actor_terminates_pending_deferred_replies(
    runtime: Runtime,
) -> None:
    cancelled = runtime.event_class()

    class EventualActorImpl(
        EventualActor,
        runtime.actor_class,  # type: ignore[name-defined]
    ):
        pass

    actor_ref = EventualActorImpl.start(cancelled)
    reply = actor_ref.ask(Query(), block=False)

    assert actor_ref.stop()
    assert cancelled.is_set()
    with pytest.raises(ActorDeadError):
        reply.get(timeout=1)


def test_stopping_actor_ignores_cancellation_callback_failure(
    runtime: Runtime,
) -> None:
    def cancel() -> None:
        raise RuntimeError("cancel failed")

    class FailingCallbackActor(
        EventualActor,
        runtime.actor_class,  # type: ignore[name-defined]
    ):
        def __init__(self) -> None:
            super().__init__()
            self.reply = DeferredReplyHandle(on_cancel=cancel)

        def on_receive(self, message: Any) -> Any:
            if isinstance(message, Query):
                return self.reply
            return super().on_receive(message)

    actor_ref = FailingCallbackActor.start()
    reply = actor_ref.ask(Query(), block=False)

    assert actor_ref.stop()
    with pytest.raises(ActorDeadError):
        reply.get(timeout=1)


def test_explicit_cancellation_callback_failure_uses_failure_hook(
    runtime: Runtime,
) -> None:
    failed = runtime.event_class()

    def cancel() -> None:
        raise RuntimeError("cancel failed")

    class FailingExplicitCancelActor(
        EventualActor,
        runtime.actor_class,  # type: ignore[name-defined]
    ):
        def __init__(self) -> None:
            super().__init__()
            self.reply = DeferredReplyHandle(on_cancel=cancel)

        def on_failure(self, *args: Any) -> None:
            failed.set()

        def on_receive(self, message: Any) -> Any:
            if isinstance(message, Query):
                return self.reply
            if isinstance(message, Cancel):
                assert self.reply is not None
                self.reply.cancel()
                self.reply = None
                return None
            return super().on_receive(message)

    actor_ref = FailingExplicitCancelActor.start()
    reply = actor_ref.ask(Query(), block=False)

    actor_ref.tell(Cancel())

    with pytest.raises(CancelledError):
        reply.get(timeout=1)
    assert failed.wait(1)
    assert not actor_ref.is_alive()
