from __future__ import annotations

import threading
import time
from typing import Any, ClassVar

import pytest

import pykka
from pykka import (
    ActorDeadError,
    ActorRegistry,
    ChildSpec,
    RestartStrategy,
    Supervisor,
    ThreadingActor,
)

pytestmark = pytest.mark.usefixtures("_stop_all")


def _ask_until_pong(ref: pykka.ActorRef[Any], *, timeout: float = 5.0) -> str:
    """Poll an actor through a restart until it answers again."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = ref.ask("ping")
        except (ActorDeadError, pykka.Timeout):
            time.sleep(0.01)
            continue
        if result == "pong":
            return result
        time.sleep(0.01)
    raise AssertionError("actor never became responsive after restart")


class CrashableChild(ThreadingActor):
    incarnations: ClassVar[list[threading.Event]] = []  # type: ignore[assignment]

    started_event: threading.Event
    fail_on_start_count: int

    def __init__(
        self,
        started_event: threading.Event | None = None,
        fail_on_start_count: int = 0,
    ) -> None:
        super().__init__()
        self.started_event = started_event or threading.Event()
        self.fail_on_start_count = fail_on_start_count
        CrashableChild.incarnations.append(self.started_event)

    def on_start(self) -> None:
        if len(CrashableChild.incarnations) <= self.fail_on_start_count:
            raise RuntimeError("on_start boom")
        self.started_event.set()

    def on_receive(self, message: Any) -> Any:
        if message == "ping":
            return "pong"
        if message == "crash":
            raise RuntimeError("receive boom")
        return None


class OtherChild(ThreadingActor):
    def __init__(self, event: threading.Event) -> None:
        super().__init__()
        self.event = event

    def on_start(self) -> None:
        self.event.set()

    def on_receive(self, message: Any) -> Any:
        if message == "ping":
            return "other-pong"
        return None


class OneForOneParent(Supervisor, ThreadingActor):
    max_restarts = 3
    within_time_range = 10.0

    started_event: threading.Event
    sibling_event: threading.Event
    gave_up: threading.Event
    gave_up_name: str | None

    def __init__(
        self,
        started_event: threading.Event,
        sibling_event: threading.Event,
        gave_up: threading.Event,
    ) -> None:
        super().__init__()
        self.started_event = started_event
        self.sibling_event = sibling_event
        self.gave_up = gave_up
        self.gave_up_name = None

    def children(self) -> list[ChildSpec]:
        return [
            ChildSpec(CrashableChild, kwargs={"started_event": self.started_event}),
            ChildSpec(OtherChild, kwargs={"event": self.sibling_event}),
        ]

    def on_restart_intensity_reached(
        self,
        child: ChildSpec,
        exc_info: object,
    ) -> None:
        self.gave_up_name = child.name
        self.gave_up.set()


@pytest.fixture(autouse=True)
def _reset_crashable_child() -> Any:
    CrashableChild.incarnations = []
    yield
    CrashableChild.incarnations = []


#
# Lifecycle basics
#


def test_children_started_with_parent_and_registered() -> None:
    child_event = threading.Event()
    sibling_event = threading.Event()
    parent = OneForOneParent.start(child_event, sibling_event, threading.Event())

    assert child_event.wait(3)
    assert sibling_event.wait(3)
    classes = {r.actor_class.__name__ for r in ActorRegistry.get_all()}
    assert classes == {"CrashableChild", "OneForOneParent", "OtherChild"}
    assert parent.is_alive()


def test_business_exception_from_ask_does_not_cause_restart() -> None:
    child_event = threading.Event()
    sibling_event = threading.Event()
    parent = OneForOneParent.start(child_event, sibling_event, threading.Event())
    child_event.wait(3)
    ref = parent.proxy().child_ref("CrashableChild").get()
    incarnation_count = len(CrashableChild.incarnations)

    with pytest.raises(RuntimeError, match="receive boom"):
        ref.ask("crash")

    # The exception is returned to the caller; the actor is still alive and no
    # new incarnation was started.
    assert ref.is_alive()
    assert ref.ask("ping") == "pong"
    assert len(CrashableChild.incarnations) == incarnation_count


def test_tell_failure_restarts_and_old_ref_follows_new_incarnation() -> None:
    child_event = threading.Event()
    sibling_event = threading.Event()
    parent = OneForOneParent.start(child_event, sibling_event, threading.Event())
    child_event.wait(3)
    ref = parent.proxy().child_ref("CrashableChild").get()
    urn = ref.actor_urn

    ref.tell("crash")

    assert _ask_until_pong(ref) == "pong"
    assert ref.actor_urn == urn  # stable identity follows the new incarnation
    assert len(CrashableChild.incarnations) == 2
    assert ActorRegistry.get_by_urn(urn) is ref


#
# Restart intensity
#


def test_continuous_failures_hit_restart_intensity_cap() -> None:
    child_event = threading.Event()
    sibling_event = threading.Event()
    gave_up = threading.Event()
    parent = OneForOneParent.start(child_event, sibling_event, gave_up)
    child_event.wait(3)
    ref = parent.proxy().child_ref("CrashableChild").get()
    urn = ref.actor_urn

    for _ in range(10):
        if not ref.is_alive():
            break
        ref.tell("crash")
        time.sleep(0.05)

    assert gave_up.wait(5)
    assert not ref.is_alive()
    assert ActorRegistry.get_by_urn(urn) is None
    # Initial incarnation + max_restarts restarts.
    assert len(CrashableChild.incarnations) == 1 + OneForOneParent.max_restarts
    assert parent.is_alive()


def test_restart_budget_recovers_after_window() -> None:
    class SlowWindowParent(OneForOneParent):
        within_time_range = 0.2

    child_event = threading.Event()
    parent = SlowWindowParent.start(
        child_event, threading.Event(), threading.Event()
    )
    child_event.wait(3)
    ref = parent.proxy().child_ref("CrashableChild").get()

    ref.tell("crash")
    assert _ask_until_pong(ref) == "pong"
    count_after_first = len(CrashableChild.incarnations)
    time.sleep(0.3)  # let the intensity window slide

    ref.tell("crash")
    assert _ask_until_pong(ref) == "pong"
    # Window elapsed, so this restart must not be counted against the first one.
    assert len(CrashableChild.incarnations) == count_after_first + 1
    parent.stop()


#
# Single-child recovery / strategy isolation
#


def test_one_for_one_restarts_only_failing_child() -> None:
    child_event = threading.Event()
    sibling_event = threading.Event()
    parent = OneForOneParent.start(child_event, sibling_event, threading.Event())
    child_event.wait(3)
    sibling_event.wait(3)

    failing = parent.proxy().child_ref("CrashableChild").get()
    sibling = parent.proxy().child_ref("OtherChild").get()

    failing.tell("crash")
    assert _ask_until_pong(failing) == "pong"

    # The sibling must be untouched and keep answering on its original ref.
    assert sibling.ask("ping") == "other-pong"
    assert sibling.is_alive()


def test_all_for_one_restarts_siblings_as_well() -> None:
    first_event = threading.Event()
    second_event = threading.Event()

    class AllForOneParent(Supervisor, ThreadingActor):
        restart_strategy = RestartStrategy.ALL_FOR_ONE

        def children(self) -> list[ChildSpec]:
            return [
                ChildSpec(CrashableChild, kwargs={"started_event": first_event}),
                ChildSpec(OtherChild, kwargs={"event": second_event}),
            ]

    parent = AllForOneParent.start()
    first_event.wait(3)
    second_event.wait(3)

    failing = parent.proxy().child_ref("CrashableChild").get()
    sibling = parent.proxy().child_ref("OtherChild").get()
    failing.tell("crash")

    assert _ask_until_pong(failing) == "pong"
    assert sibling.ask("ping") == "other-pong"
    # OtherChild gets a second on_start as part of the group restart.
    assert second_event.is_set()
    parent.stop()


#
# Shutdown
#


def test_parent_does_not_restart_children_during_shutdown_and_stops_reverse() -> None:
    order: list[str] = []
    order_lock = threading.Lock()
    restarts: list[threading.Event] = []

    class OrderChild(ThreadingActor):
        def __init__(self, name: str) -> None:
            super().__init__()
            self.name = name
            restarts.append(threading.Event())

        def on_start(self) -> None:
            restarts[-1].set()

        def on_stop(self) -> None:
            with order_lock:
                order.append(self.name)

        def on_receive(self, message: Any) -> Any:
            if message == "crash":
                raise RuntimeError("boom at shutdown")
            return None

    class OrderedParent(Supervisor, ThreadingActor):
        def children(self) -> list[ChildSpec]:
            return [
                ChildSpec(OrderChild, name="a", kwargs={"name": "a"}),
                ChildSpec(OrderChild, name="b", kwargs={"name": "b"}),
                ChildSpec(OrderChild, name="c", kwargs={"name": "c"}),
            ]

    parent = OrderedParent.start()
    time.sleep(0.3)

    # Crash one child; it gets restarted normally before shutdown.
    first = parent.proxy().child_ref("a").get()
    first.tell("crash")
    time.sleep(0.3)
    incarnations_before_shutdown = len(restarts)

    parent.stop()
    parent.actor_stopped.wait(3)
    time.sleep(0.3)

    assert not parent.is_alive()
    # Children stopped in reverse declaration order.
    assert order == ["c", "b", "a"]
    # No child was (re)started after the supervisor began shutting down.
    assert len(restarts) == incarnations_before_shutdown
    assert ActorRegistry.get_all() == []


#
# Startup failure
#


def test_child_start_failure_does_not_leave_orphans() -> None:
    class FailingOnStart(ThreadingActor):
        def on_start(self) -> None:
            raise RuntimeError("cannot start")

    class HealthyChild(ThreadingActor):
        def __init__(self, event: threading.Event) -> None:
            super().__init__()
            self.event = event

        def on_start(self) -> None:
            self.event.set()

    healthy_event = threading.Event()

    class FailingParent(Supervisor, ThreadingActor):
        def children(self) -> list[ChildSpec]:
            return [
                ChildSpec(HealthyChild, kwargs={"event": healthy_event}),
                ChildSpec(FailingOnStart),
            ]

    parent = FailingParent.start()
    assert healthy_event.wait(3)
    parent.actor_stopped.wait(3)

    assert not parent.is_alive()
    time.sleep(0.2)
    # The healthy sibling started first is torn down; nothing is left behind.
    assert ActorRegistry.get_all() == []


def test_constructor_failure_during_restart_gives_up_not_crashes_parent() -> None:
    class FlakyCtor(ThreadingActor):
        attempts = 0

        def __init__(self) -> None:
            super().__init__()
            FlakyCtor.attempts += 1
            if FlakyCtor.attempts >= 2:
                raise RuntimeError("ctor boom")

        def on_receive(self, message: Any) -> Any:
            if message == "crash":
                raise RuntimeError("receive boom")
            return None

    class Parent(Supervisor, ThreadingActor):
        def children(self) -> list[ChildSpec]:
            return [ChildSpec(FlakyCtor)]

    parent = Parent.start()
    time.sleep(0.2)
    ref = parent.proxy().child_ref("FlakyCtor").get()
    ref.tell("crash")
    time.sleep(0.5)

    assert parent.is_alive()
    assert not ref.is_alive()
    parent.stop()
