from __future__ import annotations

import threading
import time
from typing import Any

import pytest

import pykka
from pykka import (
    ActorDeadError,
    ActorRegistry,
    Child,
    RestartStrategy,
    Supervisor,
    SupervisorStrategy,
    ThreadingActor,
)

pytestmark = pytest.mark.usefixtures("_stop_all")


class Worker(ThreadingActor):
    incarnations: int = 0
    starts: "threading.Event"
    stops: "threading.Event"

    def __init__(
        self,
        starts: threading.Event | None = None,
        stops: threading.Event | None = None,
        fail_on_start: bool = False,
    ) -> None:
        super().__init__()
        self._starts = starts
        self._stops = stops
        self._fail_on_start = fail_on_start

    def on_start(self) -> None:
        Worker.incarnations += 1
        if self._starts is not None:
            self._starts.set()
        if self._fail_on_start:
            raise RuntimeError("on_start boom")

    def on_stop(self) -> None:
        if self._stops is not None:
            self._stops.set()

    def on_receive(self, message: Any) -> Any:
        if message == "boom":
            raise RuntimeError("boom")
        if message == "ping":
            return "pong"
        if isinstance(message, dict) and message.get("command") == "raise":
            raise ValueError("business error")
        return super().on_receive(message)


class FailingInitWorker(ThreadingActor):
    def __init__(self) -> None:
        super().__init__()
        raise RuntimeError("init boom")


class IdleWorker(ThreadingActor):
    def on_receive(self, message: Any) -> Any:
        if message == "ping":
            return "pong"
        return None


@pytest.fixture(autouse=True)
def _reset_worker_counters() -> Any:
    Worker.incarnations = 0
    yield


def make_supervisor(
    children: tuple[Child, ...],
    strategy: SupervisorStrategy | None = None,
    *,
    on_failure_event: threading.Event | None = None,
    on_start_raises: bool = False,
    starts: threading.Event | None = None,
    stops: threading.Event | None = None,
) -> type:
    class TestSupervisor(Supervisor, ThreadingActor):
        pass

    TestSupervisor.children = children
    TestSupervisor.strategy = strategy or SupervisorStrategy(
        max_restarts=3,
        within_seconds=5,
    )

    def on_supervision_failure(
        self: Any,
        name: str,
        exception: BaseException | None,
    ) -> None:
        if on_failure_event is not None:
            on_failure_event.set()

    if on_failure_event is not None:
        TestSupervisor.on_supervision_failure = (  # type: ignore[assignment]
            on_supervision_failure
        )

    if on_start_raises:
        def on_start(self: Any) -> None:
            if starts is not None:
                starts.set()
            raise RuntimeError("supervisor on_start boom")

        TestSupervisor.on_start = on_start  # type: ignore[assignment]

    if stops is not None:
        def on_stop(self: Any) -> None:
            stops.set()

        TestSupervisor.on_stop = on_stop  # type: ignore[assignment]

    return TestSupervisor


def child_refs(class_name: str = "Worker") -> list[pykka.ActorRef[Any]]:
    return [
        ref
        for ref in ActorRegistry.get_all()
        if ref.actor_class.__name__ == class_name
    ]


def wait_for(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_children_are_started_in_declaration_order() -> None:
    supervisor_cls = make_supervisor(
        (
            Child(IdleWorker, name="first"),
            Child(IdleWorker, name="second"),
        )
    )
    ref = supervisor_cls.start()

    assert wait_for(lambda: len(ActorRegistry.get_all()) == 3)
    names_in_registry = [str(r) for r in ActorRegistry.get_all()]
    assert any("TestSupervisor" in name for name in names_in_registry)
    ref.stop()
    assert ActorRegistry.get_all() == []


def test_ask_exception_does_not_restart_actor() -> None:
    starts = threading.Event()
    supervisor_cls = make_supervisor(
        (Child(Worker, name="worker", kwargs={"starts": starts}),)
    )
    supervisor_cls.start()
    assert starts.wait(5)

    ref = child_refs()[0]
    with pytest.raises(ValueError, match="business error"):
        ref.ask({"command": "raise"})

    time.sleep(0.2)
    assert ref.is_alive()
    assert Worker.incarnations == 1
    assert ref.ask("ping") == "pong"


def test_lifecycle_failure_restarts_child_and_ref_follows_new_instance() -> None:
    starts = threading.Event()
    supervisor_cls = make_supervisor(
        (Child(Worker, name="worker", kwargs={"starts": starts}),)
    )
    supervisor_cls.start()
    assert starts.wait(5)

    ref = child_refs()[0]
    urn = ref.actor_urn
    assert ref.ask("ping") == "pong"

    ref.tell("boom")

    assert wait_for(lambda: Worker.incarnations == 2)
    assert ref.is_alive()
    assert ref.actor_urn == urn
    assert ref.ask("ping") == "pong"


def test_one_for_one_only_restarts_the_failing_child() -> None:
    starts = {"a": 0, "b": 0}
    starts_lock = threading.Lock()

    class NamedWorker(ThreadingActor):
        def __init__(self, label: str) -> None:
            super().__init__()
            self.label = label

        def on_start(self) -> None:
            with starts_lock:
                starts[self.label] += 1

        def on_receive(self, message: Any) -> Any:
            if message == "boom":
                raise RuntimeError("boom")
            return None

    supervisor_cls = make_supervisor(
        (
            Child(NamedWorker, name="a", kwargs={"label": "a"}),
            Child(NamedWorker, name="b", kwargs={"label": "b"}),
        ),
        SupervisorStrategy(
            max_restarts=3,
            within_seconds=5,
            strategy=RestartStrategy.ONE_FOR_ONE,
        ),
    )
    supervisor_cls.start()
    assert wait_for(lambda: sum(starts.values()) == 2)

    refs = child_refs("NamedWorker")
    refs[0].tell("boom")

    assert wait_for(lambda: sum(starts.values()) == 3)
    time.sleep(0.1)
    # Exactly one of the two children got an additional incarnation.
    assert sum(starts.values()) == 3
    assert all(ref.is_alive() for ref in refs)


def test_all_for_one_restarts_all_children() -> None:
    stop_count = 0
    stop_lock = threading.Lock()

    class StoppableWorker(ThreadingActor):
        def on_stop(self) -> None:
            nonlocal stop_count
            with stop_lock:
                stop_count += 1

        def on_receive(self, message: Any) -> Any:
            if message == "boom":
                raise RuntimeError("boom")
            return None

    supervisor_cls = make_supervisor(
        (
            Child(StoppableWorker, name="a"),
            Child(StoppableWorker, name="b"),
        ),
        SupervisorStrategy(
            max_restarts=3,
            within_seconds=5,
            strategy=RestartStrategy.ALL_FOR_ONE,
        ),
    )
    supervisor_cls.start()
    assert wait_for(lambda: len(ActorRegistry.get_all()) == 3)

    refs = child_refs("StoppableWorker")
    refs[0].tell("boom")

    assert wait_for(lambda: stop_count >= 1)
    time.sleep(0.1)
    assert all(ref.is_alive() for ref in refs)
    # The healthy sibling was restarted gracefully (on_stop); the failed
    # child was restarted directly without on_stop.
    assert stop_count == 1


def test_continuous_failures_hit_max_restarts_within_window() -> None:
    gave_up = threading.Event()
    supervisor_cls = make_supervisor(
        (Child(Worker, name="worker"),),
        SupervisorStrategy(max_restarts=3, within_seconds=60),
        on_failure_event=gave_up,
    )
    supervisor_cls.start()
    assert wait_for(lambda: Worker.incarnations == 1)

    ref = child_refs()[0]
    for _ in range(6):
        try:
            ref.tell("boom")
        except ActorDeadError:
            pass
        time.sleep(0.05)

    assert gave_up.wait(5)
    assert not ref.is_alive()
    # Initial incarnation + 3 restarts = 4 incarnations total.
    assert Worker.incarnations == 4
    with pytest.raises(ActorDeadError):
        ref.ask("ping")


def test_restart_budget_window_slides() -> None:
    supervisor_cls = make_supervisor(
        (Child(Worker, name="worker"),),
        SupervisorStrategy(max_restarts=2, within_seconds=0.2),
    )
    supervisor_cls.start()
    assert wait_for(lambda: len(child_refs()) == 1)
    ref = child_refs()[0]

    ref.tell("boom")
    assert wait_for(lambda: Worker.incarnations == 2)
    time.sleep(0.3)
    ref.tell("boom")
    assert wait_for(lambda: Worker.incarnations == 3)
    assert ref.is_alive()


def test_unlimited_restarts() -> None:
    supervisor_cls = make_supervisor(
        (Child(Worker, name="worker"),),
        SupervisorStrategy(max_restarts=None),
    )
    supervisor_cls.start()
    assert wait_for(lambda: len(child_refs()) == 1)
    ref = child_refs()[0]

    for _ in range(6):
        ref.tell("boom")
        time.sleep(0.02)

    assert wait_for(lambda: Worker.incarnations >= 7)
    assert ref.is_alive()


def test_child_recovers_after_single_failure() -> None:
    supervisor_cls = make_supervisor((Child(Worker, name="worker"),))
    supervisor_cls.start()
    assert wait_for(lambda: len(child_refs()) == 1)
    ref = child_refs()[0]

    ref.tell("boom")
    assert wait_for(lambda: Worker.incarnations == 2)

    assert ref.ask("ping") == "pong"
    time.sleep(0.1)
    assert Worker.incarnations == 2


def test_parent_stops_children_in_reverse_order_during_shutdown() -> None:
    stop_order: list[str] = []
    order_lock = threading.Lock()

    class OrderedWorker(ThreadingActor):
        label: str

        def on_stop(self) -> None:
            with order_lock:
                stop_order.append(self.label)

    children = tuple(
        Child(
            type(
                f"OrderedWorker{i}",
                (OrderedWorker,),
                {"label": f"c{i}"},
            ),
            name=f"c{i}",
        )
        for i in range(3)
    )
    supervisor_cls = make_supervisor(children)
    ref = supervisor_cls.start()
    assert wait_for(lambda: len(ActorRegistry.get_all()) == 4)

    ref.stop()

    assert wait_for(lambda: stop_order == ["c2", "c1", "c0"])
    assert ActorRegistry.get_all() == []


def test_no_restart_during_parent_shutdown() -> None:
    supervisor_cls = make_supervisor((Child(Worker, name="worker"),))
    ref = supervisor_cls.start()
    assert wait_for(lambda: Worker.incarnations == 1)
    child = child_refs()[0]

    # Kill the child right as the parent shuts down; it must not be
    # restarted and must be fully stopped.
    child.tell("boom")
    ref.stop(timeout=10)

    assert Worker.incarnations == 1
    assert not child.is_alive()
    assert ActorRegistry.get_all() == []


def test_failing_child_during_parent_shutdown_is_not_restarted() -> None:
    # A slower variant: crash first (restart starts), then stop the parent.
    supervisor_cls = make_supervisor((Child(Worker, name="worker"),))
    ref = supervisor_cls.start()
    child = child_refs()[0]

    child.tell("boom")
    assert wait_for(lambda: Worker.incarnations == 2)
    child.tell("boom")
    ref.stop(timeout=10)

    assert ActorRegistry.get_all() == []


def test_child_start_failure_leaves_no_orphans() -> None:
    supervisor_cls = make_supervisor(
        (
            Child(IdleWorker, name="good"),
            Child(
                Worker,
                name="bad",
                kwargs={"fail_on_start": True},
            ),
        ),
        SupervisorStrategy(max_restarts=0, within_seconds=5),
    )
    supervisor_cls.start()

    assert wait_for(lambda: ActorRegistry.get_all() == [])


def test_child_constructor_failure_leaves_no_orphans() -> None:
    supervisor_cls = make_supervisor(
        (
            Child(IdleWorker, name="good"),
            Child(FailingInitWorker, name="bad"),
        )
    )
    supervisor_cls.start()

    assert wait_for(lambda: ActorRegistry.get_all() == [])


def test_supervisor_on_start_failure_stops_children() -> None:
    supervisor_starts = threading.Event()
    supervisor_stops = threading.Event()
    supervisor_cls = make_supervisor(
        (Child(IdleWorker, name="child"),),
        on_start_raises=True,
        starts=supervisor_starts,
        stops=supervisor_stops,
    )
    supervisor_cls.start()

    assert supervisor_starts.wait(5)
    assert wait_for(lambda: ActorRegistry.get_all() == [])


def test_unsupervised_actor_behavior_is_unchanged() -> None:
    ref = Worker.start()
    assert ref.ask("ping") == "pong"
    ref.tell("boom")
    assert wait_for(lambda: not ref.is_alive())
    assert ActorRegistry.get_all() == []


def test_per_child_strategy_overrides_supervisor_default() -> None:
    gave_up = threading.Event()
    supervisor_cls = make_supervisor(
        (
            Child(
                Worker,
                name="worker",
                strategy=SupervisorStrategy(max_restarts=1, within_seconds=60),
            ),
        ),
        SupervisorStrategy(max_restarts=10, within_seconds=60),
        on_failure_event=gave_up,
    )
    supervisor_cls.start()
    assert wait_for(lambda: len(child_refs()) == 1)
    ref = child_refs()[0]

    for _ in range(4):
        try:
            ref.tell("boom")
        except ActorDeadError:
            pass
        time.sleep(0.05)

    assert gave_up.wait(5)
    assert Worker.incarnations == 2
    assert not ref.is_alive()


def test_stop_blocking_returns_true_for_supervised_child() -> None:
    supervisor_cls = make_supervisor((Child(IdleWorker, name="worker"),))
    supervisor_cls.start()
    assert wait_for(lambda: len(child_refs("IdleWorker")) == 1)
    ref = child_refs("IdleWorker")[0]

    result = ref.stop(block=True, timeout=5)
    assert result is True
