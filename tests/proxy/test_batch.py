from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest

import pykka
from pykka import Actor

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pykka import ActorProxy
    from tests.types import Runtime


class NestedCounter:
    pykka_traversable = True

    def __init__(self) -> None:
        self.value = 100


class CounterActor(Actor):
    nested = NestedCounter()

    def __init__(self) -> None:
        super().__init__()
        self.count = 0
        self.log: list[str] = []

    def increment(self, by: int = 1) -> int:
        self.count += by
        return self.count

    def fail(self) -> None:
        msg = "boom"
        raise ValueError(msg)

    def record(self, name: str, delay: float = 0.0) -> str:
        if delay:
            time.sleep(delay)
        self.log.append(name)
        return name

    def block_on_own_future(self) -> Any:
        return self.actor_ref.proxy().count.get()

    def on_receive(self, message: Any) -> Any:
        self.log.append(message)
        return message


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


def test_batch_returns_results_in_operation_order(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.count
    batch.increment(5)
    batch.count
    batch.count = 10
    batch.count

    assert batch.execute().get() == [0, 5, 5, None, 10]
    assert proxy.count.get() == 10


def test_batch_can_block_for_the_results_directly(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment()
    batch.count

    assert batch.execute(block=True) == [1, 1]


def test_batch_is_executed_without_interleaved_messages(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.record("a", delay=0.05)
    batch.record("b")
    batch.record("c")
    future = batch.execute()

    proxy.actor_ref.tell("external")

    assert future.get() == ["a", "b", "c"]
    assert proxy.log.get() == ["a", "b", "c", "external"]


def test_batch_stop_on_error_aborts_remaining_operations(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.increment()
    batch.fail()
    batch.increment()

    with pytest.raises(ValueError, match="boom"):
        batch.execute().get()

    # The side effect of the operation before the failure remains, and the
    # operations after the failure were never executed.
    assert proxy.count.get() == 1


def test_batch_continue_on_error_collects_exceptions(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch(on_error="continue")
    batch.increment()
    batch.fail()
    batch.increment(10)

    results = batch.execute().get()

    assert results[0] == 1
    assert isinstance(results[1], ValueError)
    assert results[2] == 11
    assert proxy.count.get() == 11


def test_batch_rejects_blocking_on_own_actors_future(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.block_on_own_future()

    with pytest.raises(RuntimeError, match="deadlock"):
        batch.execute().get()

    # The actor survived and is still responsive.
    assert proxy.increment().get() == 1


def test_batch_continue_on_error_collects_deadlock_guard_error(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch(on_error="continue")
    batch.block_on_own_future()
    batch.increment()

    results = batch.execute().get()

    assert isinstance(results[0], RuntimeError)
    assert results[1] == 1


def test_batch_follows_traversable_attribute_paths(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()
    batch.nested.value
    batch.nested.value = 42
    batch.nested.value

    assert batch.execute().get() == [100, None, 42]
    assert proxy.nested.value.get() == 42


def test_batch_raises_for_unknown_attribute(
    proxy: ActorProxy[CounterActor],
) -> None:
    batch = proxy.batch()

    with pytest.raises(AttributeError):
        batch.no_such_attribute


def test_batch_raises_for_invalid_on_error(
    proxy: ActorProxy[CounterActor],
) -> None:
    with pytest.raises(ValueError, match="on_error"):
        proxy.batch(on_error="ignore")  # type: ignore[arg-type]


def test_batch_raises_when_executing_without_operations(
    proxy: ActorProxy[CounterActor],
) -> None:
    with pytest.raises(ValueError, match="no operations"):
        proxy.batch().execute()
