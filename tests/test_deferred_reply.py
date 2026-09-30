from __future__ import annotations

import queue
from typing import TYPE_CHECKING, Any

import pytest

import pykka

if TYPE_CHECKING:
    from pykka import ActorRef, DeferredReply
    from tests.types import Runtime


class DeferredReplyActor(pykka.Actor):
    def __init__(self) -> None:
        super().__init__()
        self.pending_replies: dict[str, DeferredReply[str]] = {}
        self.completed_replies: dict[str, DeferredReply[str]] = {}
        self.cancelled_queries: list[str] = []

    def on_receive(self, message: dict[str, Any]) -> Any:
        command = message["command"]

        if command == "query":
            query_id = message["id"]
            reply = pykka.DeferredReply(
                on_cancel=lambda: self.actor_ref.tell(
                    {"command": "cancel", "id": query_id}
                )
            )
            self.pending_replies[query_id] = reply
            return reply

        if command == "update":
            query_id = message["id"]
            reply = self.pending_replies.pop(query_id)
            self.completed_replies[query_id] = reply
            reply.set(message["value"])
            return None

        if command == "fail":
            query_id = message["id"]
            try:
                raise RuntimeError("query failed")
            except RuntimeError:
                reply = self.pending_replies.pop(query_id)
                self.completed_replies[query_id] = reply
                reply.set_exception()
            return None

        if command == "cancel":
            self.pending_replies.pop(message["id"], None)
            self.cancelled_queries.append(message["id"])
            return None

        if command == "status":
            return self.cancelled_queries

        if command == "complete_again":
            try:
                self.completed_replies[message["id"]].set("second value")
            except queue.Full:
                return "full"
            return None

        return super().on_receive(message)


@pytest.fixture
def actor_ref(runtime: Runtime) -> ActorRef[DeferredReplyActor]:
    class DeferredReplyActorImpl(  # type: ignore[misc]
        DeferredReplyActor,
        runtime.actor_class,
    ):
        pass

    ref = DeferredReplyActorImpl.start()
    yield ref
    ref.stop()


def test_query_does_not_block_update_that_completes_it(
    actor_ref: ActorRef[DeferredReplyActor],
) -> None:
    query = actor_ref.ask({"command": "query", "id": "1"}, block=False)

    assert actor_ref.ask({"command": "status"}) == []

    actor_ref.tell({"command": "update", "id": "1", "value": "updated"})

    assert query.get(timeout=1) == "updated"


def test_timeout_does_not_cancel_business_operation(
    actor_ref: ActorRef[DeferredReplyActor],
) -> None:
    query = actor_ref.ask({"command": "query", "id": "timeout"}, block=False)

    with pytest.raises(pykka.Timeout):
        query.get(timeout=0)

    actor_ref.tell({"command": "update", "id": "timeout", "value": "still done"})

    assert query.get(timeout=1) == "still done"


def test_cancel_is_explicit_and_notifies_actor_once(
    actor_ref: ActorRef[DeferredReplyActor],
) -> None:
    query = actor_ref.ask({"command": "query", "id": "cancel"}, block=False)

    assert query.cancel() is True
    assert query.cancel() is False

    with pytest.raises(pykka.CancelledError):
        query.get(timeout=1)

    assert actor_ref.ask({"command": "status"}) == ["cancel"]


def test_cancel_before_query_is_handled_still_notifies_actor(
    runtime: Runtime,
) -> None:
    gate = runtime.event_class()

    class GatedActor(  # type: ignore[misc]
        DeferredReplyActor,
        runtime.actor_class,
    ):
        def __init__(self) -> None:
            super().__init__()
            self.gate = gate

        def on_receive(self, message: Any) -> Any:
            if isinstance(message, dict) and message.get("command") == "wait":
                self.gate.wait(5)
                return None
            return super().on_receive(message)

    ref = GatedActor.start()
    ref.tell({"command": "wait"})

    query = ref.ask({"command": "query", "id": "early-cancel"}, block=False)
    assert query.cancel() is True
    gate.set()

    assert ref.ask({"command": "status"}) == ["early-cancel"]
    ref.stop()


def test_deferred_reply_can_be_completed_with_exception(
    actor_ref: ActorRef[DeferredReplyActor],
) -> None:
    query = actor_ref.ask({"command": "query", "id": "failure"}, block=False)

    actor_ref.tell({"command": "fail", "id": "failure"})

    with pytest.raises(RuntimeError, match="query failed"):
        query.get(timeout=1)


def test_deferred_reply_can_only_be_completed_once(
    actor_ref: ActorRef[DeferredReplyActor],
) -> None:
    query = actor_ref.ask(
        {"command": "query", "id": "once"},
        block=False,
    )

    actor_ref.tell({"command": "update", "id": "once", "value": "first"})

    assert query.get(timeout=1) == "first"
    assert (
        actor_ref.ask({"command": "complete_again", "id": "once"}, timeout=1)
        == "full"
    )
    assert query.get(timeout=1) == "first"


def test_pending_deferred_replies_fail_when_actor_stops(
    runtime: Runtime,
) -> None:
    class ActorImpl(  # type: ignore[misc]
        DeferredReplyActor,
        runtime.actor_class,
    ):
        pass

    actor_ref = ActorImpl.start()
    query = actor_ref.ask({"command": "query", "id": "stopping"}, block=False)

    assert actor_ref.stop(timeout=1) is True

    with pytest.raises(pykka.ActorDeadError):
        query.get(timeout=1)


def test_deferred_reply_must_be_bound_by_actor() -> None:
    reply = pykka.DeferredReply[str]()

    with pytest.raises(RuntimeError, match="returned from on_receive"):
        reply.set("value")
