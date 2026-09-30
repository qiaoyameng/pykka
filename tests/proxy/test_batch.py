from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

import pykka
from pykka import Actor

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pykka import ActorProxy
    from tests.types import Runtime


class NestedCounter:
    def __init__(self) -> None:
        self.inner = 0

    def get_inner(self) -> int:
        return self.inner


class CounterActor(Actor):
    def __init__(self) -> None:
        super().__init__()
        self.count = 0
        self.events: list[tuple[str, object]] = []
        self.nested = pykka.traversable(NestedCounter())
        self.plain = NestedCounter()

    def increment(self, delta: int = 1) -> int:
        self.count += delta
        self.events.append(("increment", self.count))
        return self.count

    def record(self, label: str) -> str:
        self.events.append(("record", label))
        return label

    def fail(self) -> None:
        self.events.append(("fail", None))
        raise ValueError("boom")


@pytest.fixture
def actor_class(runtime: Runtime) -> type[CounterActor]:
    class CounterActorImpl(CounterActor, runtime.actor_class):  # type: ignore[name-defined]
        pass

    return CounterActorImpl


@pytest.fixture
def proxy(
    actor_class: type[CounterActor],
) -> Iterator[ActorProxy[CounterActor]]:
    proxy = actor_class.start().proxy()
    yield proxy
    proxy.stop()


def test_batch_results_are_ordered_by_position(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment(1)
    batch.increment(2)
    batch.count

    assert batch.execute().get() == [1, 3, 3]


def test_batch_attribute_write_is_recorded(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.count = 10
    batch.count

    assert batch.execute().get() == [None, 10]
    assert proxy.count.get() == 10


def test_batch_reads_are_consistent_while_external_writes_interleave(
    proxy: ActorProxy[CounterActor],
) -> None:
    stop_hammering = threading.Event()

    def hammer() -> None:
        while not stop_hammering.is_set():
            proxy.increment.defer()

    hammer_thread = threading.Thread(target=hammer)
    hammer_thread.start()
    try:
        batch = proxy.batch()
        for _ in range(20):
            batch.count
        results = batch.execute().get()
    finally:
        stop_hammering.set()
        hammer_thread.join()

    assert len(results) == 20
    assert len(set(results)) == 1


def test_batch_is_atomic_with_respect_to_external_messages(
    proxy: ActorProxy[CounterActor],
) -> None:
    proxy.record.defer("before")

    batch = proxy.batch()
    batch.record("b1")
    batch.record("b2")
    batch.record("b3")
    future = batch.execute()

    proxy.record.defer("after")

    assert future.get() == ["b1", "b2", "b3"]
    assert proxy.events.get() == [
        ("record", "before"),
        ("record", "b1"),
        ("record", "b2"),
        ("record", "b3"),
        ("record", "after"),
    ]


def test_batch_stop_on_error_aborts_without_rolling_back_side_effects(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment()
    batch.fail()
    batch.increment()

    with pytest.raises(ValueError, match="boom"):
        batch.execute(on_error="stop").get()

    # The first increment was applied and is not rolled back, the operation
    # after the failing one was never executed, and the actor is still alive.
    assert proxy.count.get() == 1


def test_batch_collect_mode_collects_exceptions_and_continues(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment()
    batch.fail()
    batch.increment()

    results = batch.execute(on_error="collect").get()

    assert results[0] == 1
    assert isinstance(results[1], ValueError)
    assert str(results[1]) == "boom"
    assert results[2] == 2
    assert proxy.count.get() == 2


def test_batch_rejects_future_arguments(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(ValueError, match="Future"):
        batch.increment(proxy.count)

    with pytest.raises(ValueError, match="Future"):
        batch.increment(delta=proxy.count)


def test_batch_traversable_attribute_can_be_used(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.nested.get_inner()

    assert batch.execute().get() == [0]


def test_batch_non_traversable_nested_attr_is_not_accessible(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(AttributeError):
        batch.plain.get_inner()


def test_batch_unknown_attr_raises_exception(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(AttributeError, match="has no attribute 'missing'"):
        batch.missing


def test_batch_private_attr_raises_exception(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(AttributeError, match="has no attribute '_private'"):
        batch._private  # noqa: SLF001, B018


def test_execute_with_no_operations_raises_exception(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(ValueError, match="no recorded operations"):
        batch.execute()


def test_execute_with_invalid_on_error_raises_exception(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.count

    with pytest.raises(ValueError, match="on_error"):
        batch.execute(on_error="invalid")  # type: ignore[arg-type]


def test_execute_consumes_the_batch(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment()

    assert batch.execute().get() == [1]

    with pytest.raises(ValueError, match="no recorded operations"):
        batch.execute()

    # The batch proxy can be reused to record a new batch.
    batch.increment()
    assert batch.execute().get() == [2]
