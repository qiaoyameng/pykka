from __future__ import annotations

import contextvars
import queue
import threading
from typing import Any

import pytest

from pykka import ThreadingActor, ThreadingFuture


trace_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "trace_id",
    default=None,
)
secret_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "secret",
    default=None,
)
actor_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "actor_context",
    default="actor-default",
)
callback_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "callback_context",
    default=None,
)
unset_trace_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "unset_trace_id"
)


class ContextRecorder(ThreadingActor):
    propagated_context_vars = (trace_var,)

    def __init__(
        self,
        records: queue.Queue[tuple[str, str | None, str | None, str | None]],
        next_actor: Any = None,
    ) -> None:
        super().__init__()
        self.records = records
        self.next_actor = next_actor

    def on_start(self) -> None:
        actor_var.set("actor-setup")

    def record(self, label: str, fail: bool = False) -> None:
        if self.next_actor is not None:
            current_trace = trace_var.get()
            trace_var.set(
                f"{current_trace}-continued" if current_trace else "continued"
            )
            self.next_actor.record(f"nested-{label}").get(timeout=5)

        actor_var.set(f"{label}-mutated")
        self.records.put(
            (
                label,
                trace_var.get(),
                secret_var.get(),
                actor_var.get(),
            )
        )
        if fail:
            raise RuntimeError(label)


class PlainRecorder(ThreadingActor):
    def __init__(
        self,
        records: queue.Queue[tuple[str, str | None, str | None]],
    ) -> None:
        super().__init__()
        self.records = records

    def record(self, label: str) -> None:
        self.records.put((label, trace_var.get(), secret_var.get()))


class SecretMutator(ThreadingActor):
    def __init__(
        self,
        records: queue.Queue[tuple[str, str | None]],
    ) -> None:
        super().__init__()
        self.records = records

    def mutate(self) -> None:
        secret_var.set("actor-local-secret")
        self.records.put(("mutate", secret_var.get()))

    def read(self) -> None:
        self.records.put(("read", secret_var.get()))


class UnsetSetupContextActor(ThreadingActor):
    propagated_context_vars = (unset_trace_var,)

    def __init__(
        self,
        records: queue.Queue[tuple[str, str | None]],
    ) -> None:
        super().__init__()
        self.records = records

    def on_start(self) -> None:
        unset_trace_var.set("actor-setup-trace")

    def read(self) -> None:
        self.records.put(("read", unset_trace_var.get(None)))


class SelfContinuingActor(ThreadingActor):
    propagated_context_vars = (trace_var,)

    def __init__(
        self,
        records: queue.Queue[tuple[str, str | None]],
        continued: threading.Event,
    ) -> None:
        super().__init__()
        self.records = records
        self.continued = continued

    def start_chain(self, label: str) -> None:
        self.records.put((label, trace_var.get()))
        self.actor_ref.tell(f"continue-{label}")

    def on_receive(self, message: Any) -> Any:
        if isinstance(message, str) and message.startswith("continue-"):
            self.records.put((message, trace_var.get()))
            self.continued.set()
            return None
        return super().on_receive(message)


def drain(queue: queue.Queue[tuple[str, ...]]) -> list[tuple[str, ...]]:
    items = []
    while not queue.empty():
        items.append(queue.get())
    return items


def test_context_is_propagated_through_nested_messages_and_restored() -> None:
    records: queue.Queue[tuple[str, str | None, str | None, str | None]] = (
        queue.Queue()
    )
    inner = ContextRecorder.start(records)
    outer = ContextRecorder.start(records, inner.proxy())
    start = threading.Barrier(2)

    def sender(trace_id: str, secret: str, fail: bool) -> None:
        trace_var.set(trace_id)
        secret_var.set(secret)
        start.wait(5)
        if fail:
            with pytest.raises(RuntimeError, match=trace_id):
                outer.proxy().record(trace_id, fail=True).get(timeout=5)
        else:
            outer.proxy().record(trace_id).get(timeout=5)

    first_sender = threading.Thread(
        target=sender,
        args=("trace-a", "secret-a", False),
    )
    second_sender = threading.Thread(
        target=sender,
        args=("trace-b", "secret-b", True),
    )

    first_sender.start()
    second_sender.start()
    first_sender.join(5)
    second_sender.join(5)

    outer.proxy().record("baseline").get(timeout=5)
    outer.stop()
    inner.stop()

    assert not first_sender.is_alive()
    assert not second_sender.is_alive()
    results = drain(records)
    assert [record for record in results if record[1] == "trace-a-continued"] == [
        ("nested-trace-a", "trace-a-continued", None, "nested-trace-a-mutated"),
        ("trace-a", "trace-a-continued", None, "trace-a-mutated"),
    ]
    assert [record for record in results if record[1] == "trace-b-continued"] == [
        ("nested-trace-b", "trace-b-continued", None, "nested-trace-b-mutated"),
        ("trace-b", "trace-b-continued", None, "trace-b-mutated"),
    ]
    assert results[-2:] == [
        ("nested-baseline", "continued", None, "nested-baseline-mutated"),
        ("baseline", "continued", None, "baseline-mutated"),
    ]
    assert all(record[2] is None for record in results)


def test_explicit_context_vars_only_propagate_listed_variables() -> None:
    records: queue.Queue[tuple[str, str | None, str | None]] = queue.Queue()
    actor = PlainRecorder.start(records)

    def send_without_propagation() -> None:
        trace_var.set("unlisted-trace")
        secret_var.set("unlisted-secret")
        actor.proxy().record("not-propagated").get(timeout=5)

    def send_with_propagation() -> None:
        trace_var.set("listed-trace")
        secret_var.set("listed-secret")
        actor.proxy().record(
            "propagated",
            context_vars=(trace_var,),
        ).get(timeout=5)

    threads = [
        threading.Thread(target=send_without_propagation),
        threading.Thread(target=send_with_propagation),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()

    actor.stop()

    assert sorted(drain(records)) == [
        ("not-propagated", None, None),
        ("propagated", "listed-trace", None),
    ]


def test_actor_can_continue_context_in_a_message_to_itself() -> None:
    records: queue.Queue[tuple[str, str | None]] = queue.Queue()
    continued = threading.Event()
    actor = SelfContinuingActor.start(records, continued)

    def sender() -> None:
        trace_var.set("self-chain")
        secret_var.set("self-secret")
        actor.proxy().start_chain("first").get(timeout=5)

    thread = threading.Thread(target=sender)
    thread.start()
    thread.join(5)

    assert continued.wait(5)
    actor.stop()

    assert drain(records) == [
        ("first", "self-chain"),
        ("continue-first", "self-chain"),
    ]


def test_context_changes_from_one_message_do_not_leak_to_the_next() -> None:
    records: queue.Queue[tuple[str, str | None]] = queue.Queue()
    actor = SecretMutator.start(records)

    actor.proxy().mutate().get(timeout=5)
    actor.proxy().read().get(timeout=5)
    actor.stop()

    assert drain(records) == [
        ("mutate", "actor-local-secret"),
        ("read", None),
    ]


def test_unset_sender_variable_replaces_baseline_for_message() -> None:
    records: queue.Queue[tuple[str, str | None]] = queue.Queue()
    actor = UnsetSetupContextActor.start(records)

    actor.proxy().read(context_vars=(unset_trace_var,)).get(timeout=5)
    actor.stop()

    assert drain(records) == [("read", None)]


def test_future_callback_uses_registrar_context_and_restores_waiter_context() -> None:
    source = ThreadingFuture[str]()
    mapped_future: queue.Queue[ThreadingFuture[str | None]] = queue.Queue()
    result: queue.Queue[tuple[str | None, str | None]] = queue.Queue()
    registered = threading.Event()
    set_result = threading.Event()

    def registrar() -> None:
        callback_var.set("registrar")
        mapped_future.put(source.map(lambda _: callback_var.get()))
        registered.set()
        set_result.wait(5)

    def waiter() -> None:
        registered.wait(5)
        future = mapped_future.get()
        callback_var.set("waiter")
        set_result.set()
        callback_result = future.get(timeout=5)
        result.put((callback_result, callback_var.get()))

    def setter() -> None:
        set_result.wait(5)
        callback_var.set("setter")
        source.set("value")

    threads = [
        threading.Thread(target=registrar),
        threading.Thread(target=waiter),
        threading.Thread(target=setter),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()

    assert result.get(timeout=5) == ("registrar", "waiter")
