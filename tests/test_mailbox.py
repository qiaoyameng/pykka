from __future__ import annotations

import threading
import time

import pytest

from pykka import ActorDeadError, Mailbox, MailboxFull, ThreadingFuture
from pykka._envelope import Envelope


def _envelope(message: object = None) -> Envelope[object]:
    return Envelope(message)


def _consume(mailbox: Mailbox, release: bool = False) -> Envelope[object] | None:
    """Pop an envelope, optionally simulating the consumer finishing it.

    In the actor, a finished envelope releases its slot atomically when the
    next envelope is taken. Tests simulate that by releasing manually.
    """
    if release:
        return mailbox.get()
    return mailbox.get(release_previous=False)


def _release_slot(mailbox: Mailbox) -> None:
    with mailbox._condition:
        mailbox._in_flight -= 1
        mailbox._condition.notify_all()


def test_unbounded_mailbox_accepts_many_envelopes() -> None:
    mailbox = Mailbox()

    for i in range(1000):
        mailbox.put(_envelope(i))

    assert mailbox.qsize() == 1000
    assert not mailbox.empty()


def test_non_positive_maxsize_is_unbounded() -> None:
    for maxsize in (0, -1):
        mailbox = Mailbox(maxsize=maxsize)

        for i in range(100):
            mailbox.put(_envelope(i))

        assert mailbox.qsize() == 100
        assert not mailbox.full()


def test_put_with_zero_timeout_raises_mailbox_full_immediately() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    with pytest.raises(MailboxFull):
        mailbox.put(_envelope("second"), 0)

    assert mailbox.qsize() == 1


def test_put_blocks_by_default_when_full_until_stopped() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    errors: list[BaseException] = []

    def waiting_put() -> None:
        try:
            mailbox.put(_envelope("second"))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=waiting_put)
    thread.start()
    time.sleep(0.05)

    assert thread.is_alive()

    mailbox.request_stop()
    thread.join(2)
    assert not thread.is_alive()
    assert isinstance(errors[0], ActorDeadError)

    assert mailbox.qsize() == 1


def test_put_with_timeout_raises_mailbox_full_on_expiry() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    started = time.monotonic()
    with pytest.raises(MailboxFull):
        mailbox.put(_envelope("second"), 0.1)
    elapsed = time.monotonic() - started

    assert elapsed >= 0.1
    assert mailbox.qsize() == 1


def test_put_with_timeout_succeeds_when_capacity_becomes_available() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    def drain() -> None:
        time.sleep(0.05)
        _consume(mailbox)
        _release_slot(mailbox)

    thread = threading.Thread(target=drain)
    thread.start()

    mailbox.put(_envelope("second"), 2.0)
    thread.join()

    envelope = _consume(mailbox)
    assert envelope.message == "second"


def test_put_with_none_timeout_blocks_until_capacity_is_available() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    def drain() -> None:
        time.sleep(0.05)
        _consume(mailbox)
        _release_slot(mailbox)

    thread = threading.Thread(target=drain)
    thread.start()

    mailbox.put(_envelope("second"), None)
    thread.join()

    assert mailbox.qsize() == 1


def test_put_raises_actor_dead_error_after_request_stop() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))
    mailbox.request_stop()

    with pytest.raises(ActorDeadError):
        mailbox.put(_envelope("second"), 0)


def test_waiting_putter_is_woken_up_by_stop() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    errors: list[BaseException] = []

    def waiting_put() -> None:
        try:
            mailbox.put(_envelope("second"), None)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=waiting_put)
    thread.start()

    time.sleep(0.05)
    mailbox.request_stop()
    thread.join(2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ActorDeadError)
    assert mailbox.qsize() == 1


def test_waiting_putter_is_woken_up_by_terminate() -> None:
    mailbox = Mailbox(maxsize=1)
    mailbox.put(_envelope("first"))

    errors: list[BaseException] = []

    def waiting_put() -> None:
        try:
            mailbox.put(_envelope("second"), 5.0)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=waiting_put)
    thread.start()

    time.sleep(0.05)
    mailbox.terminate()
    thread.join(2)

    assert not thread.is_alive()
    assert isinstance(errors[0], ActorDeadError)


def test_get_returns_enqueued_messages_while_stopping() -> None:
    mailbox = Mailbox(maxsize=3)
    mailbox.put(_envelope("a"))
    mailbox.put(_envelope("b"))
    ack = ThreadingFuture()
    mailbox.request_stop(ack)

    first = _consume(mailbox)
    second = mailbox.get()
    got = [first.message, second.message]
    assert got == ["a", "b"]
    assert mailbox.get() is None
    mailbox.finish_graceful_stop()
    assert ack.get(timeout=1) is None


def test_finish_graceful_stop_completes_all_stop_acks() -> None:
    mailbox = Mailbox()
    acks = [ThreadingFuture() for _ in range(3)]
    for ack in acks:
        mailbox.request_stop(ack)

    assert mailbox.get() is None
    mailbox.finish_graceful_stop()

    for ack in acks:
        assert ack.get(timeout=1) is None


def test_request_stop_on_closed_mailbox_fails_the_ack() -> None:
    mailbox = Mailbox()
    mailbox.request_stop()
    mailbox.get()
    mailbox.finish_graceful_stop()

    late_ack = ThreadingFuture()
    mailbox.request_stop(late_ack)

    with pytest.raises(ActorDeadError):
        late_ack.get(timeout=1)


def test_terminate_fails_pending_stop_acks() -> None:
    mailbox = Mailbox()
    ack = ThreadingFuture()
    mailbox.request_stop(ack)

    mailbox.terminate()

    with pytest.raises(ActorDeadError):
        ack.get(timeout=1)


def test_many_putters_each_message_is_either_in_or_rejected() -> None:
    capacity = 4
    producers = 8
    mailbox = Mailbox(maxsize=capacity)
    accepted: list[int] = []
    rejected: list[int] = []
    lock = threading.Lock()

    barrier = threading.Barrier(producers + 1)

    def putter(index: int) -> None:
        barrier.wait()
        try:
            mailbox.put(_envelope(index), 0)
        except MailboxFull:
            with lock:
                rejected.append(index)
        else:
            with lock:
                accepted.append(index)

    threads = [threading.Thread(target=putter, args=(i,)) for i in range(producers)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)

    assert len(accepted) == capacity
    assert len(rejected) == producers - capacity
    assert set(accepted).isdisjoint(rejected)
    assert mailbox.qsize() == capacity

    drained: list[object] = []
    for _ in range(capacity):
        envelope = mailbox.get()
        drained.append(envelope.message)
    assert sorted(drained) == sorted(accepted)
