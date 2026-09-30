from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pytest

from pykka import Future, ThreadingActor, ThreadingFuture
from pykka.messages import ProxyCall

if TYPE_CHECKING:
    from pykka import ActorRef

pytestmark = pytest.mark.usefixtures("_stop_all")

trace_id_var = contextvars.ContextVar("trace_id")
secret_var = contextvars.ContextVar("secret")
ACTOR_DEFAULT = "actor-default"
ACTOR_SECRET_DEFAULT = "actor-secret"


class ContextInspectingActor(ThreadingActor):
    context_vars = (trace_id_var,)

    def __init__(self, results: list[tuple[str, str | None, str | None]]) -> None:
        super().__init__()
        self.results = results

    def on_start(self) -> None:
        trace_id_var.set(ACTOR_DEFAULT)
        secret_var.set(ACTOR_SECRET_DEFAULT)

    def inspect(
        self,
        label: str = "inspect",
    ) -> tuple[str | None, str | None]:
        values = (trace_id_var.get(None), secret_var.get(None))
        self.results.append((label, *values))
        return values


class NestedActor(ThreadingActor):
    context_vars = (trace_id_var,)

    def __init__(
        self,
        child_ref: ActorRef[ContextInspectingActor],
        results: list[tuple[str, str | None, str | None]],
        nested_started: threading.Event,
        send_failure: threading.Event,
    ) -> None:
        super().__init__()
        self.child_ref = child_ref
        self.results = results
        self.nested_started = nested_started
        self.send_failure = send_failure

    def on_start(self) -> None:
        trace_id_var.set(ACTOR_DEFAULT)
        secret_var.set(ACTOR_SECRET_DEFAULT)

    def nested(
        self,
    ) -> tuple[
        tuple[str | None, str | None],
        tuple[str | None, str | None],
    ]:
        self.nested_started.set()
        self.send_failure.wait(5)
        child_values = self.child_ref.proxy().inspect("nested-child").get()
        values = (trace_id_var.get(None), secret_var.get(None))
        self.results.append(("nested-parent", *values))
        return child_values, values

    def inspect(
        self,
        label: str = "inspect",
    ) -> tuple[str | None, str | None]:
        values = (trace_id_var.get(None), secret_var.get(None))
        self.results.append((label, *values))
        return values

    def fail(self) -> None:
        self.inspect("failure")
        raise RuntimeError("failure while context is active")


def set_context_and_enqueue(
    enqueue: Callable[[], None],
    trace_id: str,
    secret: str,
    ready: threading.Event | None = None,
    release: threading.Event | None = None,
) -> threading.Thread:
    def run() -> None:
        trace_token = trace_id_var.set(trace_id)
        secret_token = secret_var.set(secret)
        try:
            if ready is not None:
                ready.set()
            if release is not None:
                release.wait(5)
            enqueue()
        finally:
            secret_var.reset(secret_token)
            trace_id_var.reset(trace_token)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_message_context_is_isolated_between_threads_and_restored_after_error() -> None:
    results: list[tuple[str, str | None, str | None]] = []
    nested_started = threading.Event()
    release_failure_sender = threading.Event()

    child_ref = ContextInspectingActor.start(results)
    parent_ref = NestedActor.start(
        child_ref,
        results,
        nested_started,
        release_failure_sender,
    )
    child = child_ref.proxy()
    parent = parent_ref.proxy()

    assert child.inspect("baseline-child").get(timeout=5) == (
        ACTOR_DEFAULT,
        ACTOR_SECRET_DEFAULT,
    )
    assert parent.inspect("baseline-parent").get(timeout=5) == (
        ACTOR_DEFAULT,
        ACTOR_SECRET_DEFAULT,
    )

    nested_futures: list[Future[Any]] = []
    failure_futures: list[Future[Any]] = []

    nested_sender = set_context_and_enqueue(
        lambda: nested_futures.append(parent.nested()),
        "trace-1",
        "secret-1",
    )
    nested_started.wait(5)

    failure_sender = set_context_and_enqueue(
        lambda: failure_futures.append(parent.fail()),
        "trace-2",
        "secret-2",
    )
    failure_sender.join(5)
    release_failure_sender.set()

    assert not failure_sender.is_alive()
    nested_sender.join(5)
    assert not nested_sender.is_alive()
    assert len(nested_futures) == 1
    assert len(failure_futures) == 1
    assert nested_futures[0].get(timeout=5) == (
        ("trace-1", ACTOR_SECRET_DEFAULT),
        ("trace-1", ACTOR_SECRET_DEFAULT),
    )

    with pytest.raises(RuntimeError, match="failure while context is active"):
        failure_futures[0].get(timeout=5)

    assert child.inspect("after-child").get(timeout=5) == (
        ACTOR_DEFAULT,
        ACTOR_SECRET_DEFAULT,
    )
    assert parent.inspect("after-parent").get(timeout=5) == (
        ACTOR_DEFAULT,
        ACTOR_SECRET_DEFAULT,
    )

    assert results == [
        ("baseline-child", ACTOR_DEFAULT, ACTOR_SECRET_DEFAULT),
        ("baseline-parent", ACTOR_DEFAULT, ACTOR_SECRET_DEFAULT),
        ("nested-child", "trace-1", ACTOR_SECRET_DEFAULT),
        ("nested-parent", "trace-1", ACTOR_SECRET_DEFAULT),
        ("failure", "trace-2", ACTOR_SECRET_DEFAULT),
        ("after-child", ACTOR_DEFAULT, ACTOR_SECRET_DEFAULT),
        ("after-parent", ACTOR_DEFAULT, ACTOR_SECRET_DEFAULT),
    ]

    child_ref.stop()
    parent_ref.stop()


def test_future_callbacks_use_registration_context_not_waiting_thread_context() -> None:
    source = ThreadingFuture[int]()
    started = threading.Event()
    release = threading.Event()
    mapped_futures: list[Future[tuple[int, str | None, str | None]]] = []

    def callback(value: int) -> tuple[int, str | None, str | None]:
        return value, trace_id_var.get(None), secret_var.get(None)

    def register() -> None:
        trace_token = trace_id_var.set("registered-trace")
        secret_token = secret_var.set("registered-secret")
        try:
            mapped_futures.append(source.map(callback))
        finally:
            secret_var.reset(secret_token)
            trace_id_var.reset(trace_token)

    registering_thread = threading.Thread(target=register)
    registering_thread.start()
    registering_thread.join(5)
    assert mapped_futures

    def wait_for_result() -> None:
        trace_token = trace_id_var.set("waiting-trace")
        secret_token = secret_var.set("waiting-secret")
        try:
            started.set()
            release.wait(5)
            result.append(mapped_futures[0].get(timeout=5))
        finally:
            secret_var.reset(secret_token)
            trace_id_var.reset(trace_token)

    result: list[tuple[int, str | None, str | None]] = []
    waiting_thread = threading.Thread(target=wait_for_result)
    waiting_thread.start()
    started.wait(5)

    trace_token = trace_id_var.set("setting-trace")
    secret_token = secret_var.set("setting-secret")
    try:
        source.set(42)
    finally:
        secret_var.reset(secret_token)
        trace_id_var.reset(trace_token)

    release.set()
    waiting_thread.join(5)

    assert not registering_thread.is_alive()
    assert not waiting_thread.is_alive()
    assert trace_id_var.get(None) is None
    assert secret_var.get(None) is None
    assert result == [(42, "registered-trace", "registered-secret")]


def test_explicit_non_whitelisted_context_variable_is_not_propagated() -> None:
    results: list[tuple[str, str | None, str | None]] = []
    actor_ref = ContextInspectingActor.start(results)
    trace_token = trace_id_var.set("allowed-trace")
    secret_token = secret_var.set("secret-value")
    try:
        result = actor_ref.ask(
            ProxyCall(
                attr_path=("inspect",),
                args=("explicit",),
                kwargs={},
            ),
            context_vars=(trace_id_var, secret_var),
            timeout=5,
        )
        assert result == ("allowed-trace", ACTOR_SECRET_DEFAULT)
    finally:
        secret_var.reset(secret_token)
        trace_id_var.reset(trace_token)

    actor_ref.stop()


def test_empty_context_vars_subset_disables_propagation() -> None:
    results: list[tuple[str, str | None, str | None]] = []
    actor_ref = ContextInspectingActor.start(results)
    trace_token = trace_id_var.set("disabled-trace")
    try:
        result = actor_ref.ask(
            ProxyCall(
                attr_path=("inspect",),
                args=("disabled",),
                kwargs={},
            ),
            context_vars=(),
            timeout=5,
        )
    finally:
        trace_id_var.reset(trace_token)

    assert result == (ACTOR_DEFAULT, ACTOR_SECRET_DEFAULT)
    actor_ref.stop()
