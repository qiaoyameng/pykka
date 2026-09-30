from __future__ import annotations

import threading
import time
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from pykka import ActorDeadError, MailboxFull, ThreadingActor

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pykka import ActorRef

pytestmark = pytest.mark.usefixtures("_stop_all")


class BoundedActor(ThreadingActor):
    max_queue_size = 1

    def __init__(
        self,
        gate: threading.Event | None = None,
        processed: list[Any] | None = None,
    ) -> None:
        super().__init__()
        self._gate = gate
        self._processed = processed

    def on_receive(self, message: Any) -> Any:
        if message == "block" and self._gate is not None:
            self._gate.wait(5)
        if self._processed is not None:
            self._processed.append(message)
        return message


@pytest.fixture
def blocked_actor() -> Iterator[tuple[ActorRef[BoundedActor], threading.Event]]:
    gate = threading.Event()
    processed: list[Any] = []
    ref = BoundedActor.start(gate, processed)
    ref.tell("block")
    # Wait until the actor is busy processing the blocking message, so the
    # actor's capacity is entirely held by the in-flight message.
    time.sleep(0.1)
    yield ref, gate
    gate.set()
    if ref.is_alive():
        ref.stop(timeout=5)


def test_tell_raises_mailbox_full_immediately_when_full(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, _gate = blocked_actor

    with pytest.raises(MailboxFull):
        ref.tell("rejected")


def test_tell_with_zero_timeout_raises_mailbox_full(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, _gate = blocked_actor
    with pytest.raises(MailboxFull):
        ref.tell("rejected", timeout=0)


def test_tell_raises_actor_dead_error_when_stopping(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor
    stop_future = ref.stop(block=False)
    time.sleep(0.05)

    with pytest.raises(ActorDeadError):
        ref.tell("too late", timeout=0)

    gate.set()
    assert stop_future.get(timeout=5) is True


def test_tell_waiting_for_space_succeeds_when_actor_drains(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor

    def send_later() -> None:
        ref.tell("later", timeout=5)

    thread = threading.Thread(target=send_later)
    thread.start()
    time.sleep(0.1)
    assert thread.is_alive()

    gate.set()
    thread.join(5)
    assert not thread.is_alive()

    assert ref.ask("ping") == "ping"


def test_tell_waiting_for_space_is_cancelled_when_actor_stops(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor

    errors: list[BaseException] = []

    def waiting_tell() -> None:
        try:
            ref.tell("never enqueued", timeout=10)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=waiting_tell)
    thread.start()
    time.sleep(0.1)
    assert thread.is_alive()

    # The stop signal must get through even though the mailbox is full.
    stop_future = ref.stop(block=False)
    gate.set()
    thread.join(5)

    assert stop_future.get(timeout=5) is True
    assert not thread.is_alive()
    assert isinstance(errors[0], ActorDeadError)


def test_blocking_ask_resolves_to_value_or_clear_error(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor

    result: list[Any] = []

    def waiting_ask() -> None:
        result.append(ref.ask("answer", timeout=5))

    thread = threading.Thread(target=waiting_ask)
    thread.start()
    time.sleep(0.1)
    gate.set()
    thread.join(5)

    assert result == ["answer"]


def test_blocking_ask_returns_mailbox_full_on_enqueue_timeout(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, _gate = blocked_actor

    with pytest.raises(MailboxFull):
        ref.ask("no room", timeout=0.05)


def test_nonblocking_ask_future_resolves_to_mailbox_full(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, _gate = blocked_actor

    future = ref.ask("no room", block=False, timeout=0)

    with pytest.raises(MailboxFull):
        future.get(timeout=1)


def test_nonblocking_ask_waiting_for_space_eventually_succeeds(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor

    future = ref.ask("answer", block=False, timeout=5)
    time.sleep(0.1)
    gate.set()

    assert future.get(timeout=5) == "answer"


def test_nonblocking_ask_waiting_for_space_gets_clear_error_on_stop(
) -> None:
    capacity = 2
    gates = [threading.Event() for _ in range(capacity)]
    started = [threading.Event() for _ in range(capacity)]

    class PinnedActor(ThreadingActor):
        max_queue_size = capacity

        def on_receive(self, message: Any) -> Any:
            index = int(message)
            started[index].set()
            gates[index].wait(5)
            return message

    ref = PinnedActor.start()

    # Pin the consumer on message 0.
    ref.tell("0")
    started[0].wait(5)
    # Fill the single remaining queue slot with another blocking message.
    for i in range(1, capacity):
        ref.tell(str(i))

    # Every slot is held. The enqueue is done from a background thread,
    # since even a non-blocking ask blocks its own caller while waiting for
    # mailbox capacity (only the reply wait is asynchronous).
    ask_future_holder: dict[str, Any] = {}
    producer_ready = threading.Event()

    def waiting_producer() -> None:
        producer_ready.set()
        ask_future_holder["future"] = ref.ask(
            "never enqueued",
            block=False,
            timeout=30,
        )

    producer = threading.Thread(target=waiting_producer)
    producer.start()
    producer_ready.wait(5)
    time.sleep(0.2)
    assert ref.actor_inbox.qsize() == capacity

    # Stop while every slot is still pinned. The independent stop channel
    # rejects the waiting producer without a slot becoming available.
    stop_future = ref.stop(block=False)
    producer.join(5)
    assert not producer.is_alive()
    waiting = ask_future_holder["future"]
    with pytest.raises(ActorDeadError):
        waiting.get(timeout=5)

    # Only now let the actor process and finish its pinned messages.
    for gate in gates:
        gate.set()
    assert stop_future.get(timeout=5) is True


def test_stop_completes_after_processing_all_enqueued_messages() -> None:
    gate = threading.Event()
    processed: list[Any] = []

    class TwoMessageActor(ThreadingActor):
        max_queue_size = 2

        def on_receive(self, message: Any) -> Any:
            if message == "block":
                gate.wait(5)
            processed.append(message)
            return message

    ref = TwoMessageActor.start()
    ref.tell("block")
    time.sleep(0.1)
    ref.tell("after-block")  # fills the one remaining queue slot

    stop_future = ref.stop(block=False)

    time.sleep(0.1)
    # The actor must still drain the queued message before stopping.
    assert "after-block" not in processed

    gate.set()
    assert stop_future.get(timeout=5) is True

    assert "block" in processed
    assert "after-block" in processed


def test_multiple_senders_against_full_mailbox() -> None:
    gate = threading.Event()
    processed: list[Any] = []
    capacity = 2
    producers = 12

    class CapacityActor(ThreadingActor):
        max_queue_size = capacity

        def on_receive(self, message: Any) -> Any:
            if message == "block":
                gate.wait(5)
            processed.append(message)
            return message

    ref = CapacityActor.start()
    ref.tell("block")
    time.sleep(0.1)

    outcomes: Counter[str] = Counter()
    counter_lock = threading.Lock()

    def producer() -> None:
        try:
            ref.tell("work", timeout=3)
        except MailboxFull:
            with counter_lock:
                outcomes["full"] += 1
        except ActorDeadError:
            with counter_lock:
                outcomes["dead"] += 1
        else:
            with counter_lock:
                outcomes["enqueued"] += 1

    threads = [threading.Thread(target=producer) for _ in range(producers)]
    for thread in threads:
        thread.start()
    time.sleep(0.3)

    stop_future = ref.stop(block=False)
    gate.set()
    stop_future.get(timeout=5)

    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()

    assert sum(outcomes.values()) == producers
    assert outcomes["enqueued"] <= capacity
    # Nothing is lost: every enqueued message was processed.
    assert processed.count("work") == outcomes["enqueued"]


def test_all_futures_resolve_when_stopping_while_full(
    blocked_actor: tuple[ActorRef[BoundedActor], threading.Event],
) -> None:
    ref, gate = blocked_actor

    producer_count = 10
    futures: list[Any] = [None] * producer_count
    barrier = threading.Barrier(producer_count + 1)

    def producer(index: int) -> None:
        barrier.wait()
        futures[index] = ref.ask(index, block=False, timeout=30)

    threads = [
        threading.Thread(target=producer, args=(i,))
        for i in range(producer_count)
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    time.sleep(0.2)

    ref.stop(block=False)
    gate.set()

    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()

    outcomes = 0
    for future in futures:
        try:
            future.get(timeout=5)
        except (ActorDeadError, MailboxFull):
            pass
        outcomes += 1

    assert outcomes == producer_count



class SelfTellActor(ThreadingActor):
    """Actor that tries to enqueue a message onto itself while busy.

    While the actor processes a message, its own in-flight envelope still
    holds the single capacity slot. A blocking self-enqueue would deadlock
    the only consumer, so it must fail immediately.
    """

    max_queue_size = 1

    def on_receive(self, message: Any) -> Any:
        if message == "self":
            try:
                self.actor_ref.tell("next", timeout=5)
            except MailboxFull:
                return "full"
            return "queued"
        return None


def test_actor_tell_to_itself_does_not_block_on_full_mailbox() -> None:
    ref = SelfTellActor.start()

    assert ref.ask("self", timeout=5) == "full"
    ref.stop(timeout=5)


class SelfAskActor(ThreadingActor):
    max_queue_size = 1

    def on_receive(self, message: Any) -> Any:
        if message == "self":
            future = self.actor_ref.ask("next", block=False, timeout=5)
            try:
                future.get(timeout=2)
            except MailboxFull:
                return "full"
            return "queued"
        return None


def test_actor_ask_to_itself_does_not_block_on_full_mailbox() -> None:
    ref = SelfAskActor.start()

    assert ref.ask("self", timeout=5) == "full"
    ref.stop(timeout=5)
