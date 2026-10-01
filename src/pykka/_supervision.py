"""Supervision tree support: parent actors that declare and supervise children.

A [`Supervisor`][pykka.Supervisor] declares its children as a list of
[`ChildSpec`][pykka.ChildSpec] values. The supervisor starts its children when
it starts, restarts them according to a
[`RestartStrategy`][pykka.RestartStrategy] and a restart-intensity budget, and
stops its children in reverse declaration order when it stops.

Only the threading runtime is supported.
"""

from __future__ import annotations

import abc
import enum
import itertools
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

from pykka import Actor, ActorDeadError, ActorRef, ActorRegistry
from pykka.messages import _ActorStop as _ActorStopMessage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pykka._types import OptExcInfo


__all__ = [
    "ChildSpec",
    "RestartStrategy",
    "Supervisor",
]


logger = logging.getLogger("pykka")


class RestartStrategy(enum.Enum):
    """How a supervisor reacts to one of its children failing.

    Attributes:
        ONE_FOR_ONE: restart only the child that failed.
        ALL_FOR_ONE: restart all of the supervisor's children, in declaration
            order, so that a group of collaborating children is reset together.
    """

    ONE_FOR_ONE = "one_for_one"
    ALL_FOR_ONE = "all_for_one"


@dataclass(frozen=True)
class ChildSpec:
    """Description of a single supervised child actor.

    Args:
        actor_class: the [`Actor`][pykka.Actor] subclass to instantiate.
        args: positional arguments for the child's constructor.
        kwargs: keyword arguments for the child's constructor.
        name: logical name of the child, unique within the supervisor. It is
            derived from the class name when omitted.
        start_timeout: seconds to wait for the child's
            [`on_start()`][pykka.Actor.on_start] to complete.
    """

    actor_class: type[Actor]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    start_timeout: float = 30.0


class _ChildTerminated(NamedTuple):
    """Internal message from a child to its supervisor."""

    urn: str
    incarnation: int
    failed: bool


class _Child:
    """The supervisor's bookkeeping for one declared child."""

    __slots__ = ("spec", "ref", "incarnation", "dead")

    def __init__(self, spec: ChildSpec, ref: ActorRef[Any]) -> None:
        self.spec = spec
        self.ref = ref
        self.incarnation = 0
        self.dead = False


class Supervisor(Actor, abc.ABC):
    """An actor that supervises a static set of child actors.

    Subclasses implement [`children()`][pykka.Supervisor.children] to declare
    the child specs. Override [`on_start()`][pykka.Supervisor.on_start] or
    [`on_stop()`][pykka.Supervisor.on_stop] for your own setup and cleanup,
    making sure to call `super()`.

    Restart behavior is controlled by
    [`restart_strategy`][pykka.Supervisor.restart_strategy],
    [`max_restarts`][pykka.Supervisor.max_restarts], and
    [`within_time_range`][pykka.Supervisor.within_time_range]: if more than
    `max_restarts` restarts happen within `within_time_range` seconds, the
    failing child is given up for dead and
    [`on_restart_intensity_reached()`][pykka.Supervisor.on_restart_intensity_reached]
    is called.

    The actor reference of a child is stable across restarts: a reference
    obtained before a crash follows the replacement incarnation once the
    replacement has started.
    """

    restart_strategy: RestartStrategy = RestartStrategy.ONE_FOR_ONE
    """Strategy used when a child fails."""

    max_restarts: int = 3
    """Maximum number of restarts allowed within the intensity window."""

    within_time_range: float = 10.0
    """Sliding window, in seconds, for the restart-intensity budget."""

    stop_timeout: float = 30.0
    """Seconds to wait for each child to stop during shutdown."""

    def children(self) -> Sequence[ChildSpec]:
        """Declare the supervised children, in start order.

        Override this to return the child specs. Children are started in the
        returned order and stopped in reverse order.
        """
        return []

    def _supervisor_init(self) -> None:
        self._super_children: dict[str, _Child] = {}
        self._super_by_urn: dict[str, _Child] = {}
        self._super_restart_times: list[float] = []
        self._super_stopping = False
        self._super_pending_stops: set[str] = set()

    def _before_loop_start(self) -> None:
        # Pin the supervisor's own URN before its loop starts so that the
        # reference handed to children cannot become stale.
        self.actor_ref._set_urn(self.actor_urn)  # noqa: SLF001
        self._supervisor_init()

    #
    # Lifecycle
    #

    def on_start(self) -> None:
        self._supervisor_init()
        specs = list(self.children())
        self._validate_specs(specs)
        try:
            for spec in specs:
                self._start_child(spec)
        except Exception:
            logger.exception(f"{self} failed to start its children; aborting")
            self._abort_children()
            self._super_stopping = True
            ActorRegistry.unregister(self.actor_ref)
            self.actor_stopped.set()
            raise

    def _stop(self) -> None:
        if not getattr(self, "_super_stopping", True):
            self._super_stopping = True
            self._stop_children()
        super()._stop()

    def on_stop(self) -> None:
        pass

    def on_restart_intensity_reached(
        self,
        child: ChildSpec,
        exc_info: OptExcInfo | None,
    ) -> None:
        """Hook called when a child exceeds the restart-intensity budget.

        Override to react, e.g. to escalate to your own supervisor. The child
        has already been given up; its stable reference is unregistered.
        """

    #
    # Message dispatch
    #

    def _handle_receive(self, message: Any) -> Any:
        if isinstance(message, _ChildTerminated):
            self._handle_child_terminated(message)
            return None
        return super()._handle_receive(message)

    def _handle_child_terminated(self, message: _ChildTerminated) -> None:
        self._super_pending_stops.discard(message.urn)
        child = self._super_by_urn.get(message.urn)
        if (
            child is None
            or child.dead
            or child.incarnation != message.incarnation
            or self._super_stopping
        ):
            return

        if not message.failed:
            # The child stopped itself (or was stopped externally); it must not
            # be resurrected.
            child.dead = True
            if not self._super_stopping:
                ActorRegistry.unregister(child.ref)
            return

        exc_info = None
        if not self._restart_allowed():
            self._give_up(child, exc_info)
            return

        if self.restart_strategy is RestartStrategy.ALL_FOR_ONE:
            self._restart_all(child, exc_info)
        elif not self._reincarnate(child):
            self._give_up(child, exc_info)

    #
    # (Re)starting children
    #

    def _start_child(self, spec: ChildSpec) -> _Child:
        """Synchronously start the first incarnation of a child."""
        obj = spec.actor_class(*spec.args, **spec.kwargs)
        ref = obj.actor_ref
        urn = uuid.uuid4().urn
        ref._set_urn(urn)  # noqa: SLF001
        obj._bind_to_supervision(  # noqa: SLF001
            supervisor_ref=self.actor_ref,
            stable_ref=ref,
            stable_urn=urn,
            incarnation=0,
        )
        child = _Child(spec, ref)
        self._super_children[spec.name] = child  # type: ignore[index]
        self._super_by_urn[urn] = child
        ActorRegistry.register(ref)
        try:
            obj._start_actor_loop()  # noqa: SLF001
            obj._wait_until_started(spec.start_timeout)  # noqa: SLF001
        except Exception:
            del self._super_children[spec.name]  # type: ignore[arg-type]
            del self._super_by_urn[urn]
            ActorRegistry.unregister(ref)
            raise
        return child

    def _reincarnate(self, child: _Child) -> bool:
        """Create and start the next incarnation of one child."""
        child.incarnation += 1
        spec = child.spec
        try:
            obj = spec.actor_class(*spec.args, **spec.kwargs)
            obj._bind_to_supervision(  # noqa: SLF001
                supervisor_ref=self.actor_ref,
                stable_ref=child.ref,
                stable_urn=child.ref.actor_urn,
                incarnation=child.incarnation,
            )
            spec.actor_class._start_incarnation(  # noqa: SLF001
                obj, child.ref, register=False
            )
            obj._wait_until_started(spec.start_timeout)  # noqa: SLF001
        except Exception:
            logger.exception(f"{self} failed to restart child {spec.name!r}")
            child.dead = True
            ActorRegistry.unregister(child.ref)
            return False
        return True

    def _restart_all(
        self,
        failed: _Child,
        exc_info: OptExcInfo | None,
    ) -> None:
        if not self._reincarnate(failed):
            self._give_up(failed, exc_info)
            return

        # Bring healthy siblings down first, then back up in declaration order.
        for child in self._super_children.values():
            if child is failed or child.dead:
                continue
            child.ref.stop(block=True, timeout=self.stop_timeout)

        for child in self._super_children.values():
            if child is failed or child.dead:
                continue
            if not self._restart_allowed():
                self._give_up(child, exc_info)
                return
            if not self._reincarnate(child):
                self._give_up(child, exc_info)
                return

    #
    # Stopping / aborting
    #

    def _stop_children(self) -> None:
        """Stop children in reverse order, waiting for each to terminate.

        Runs in the supervisor's own thread, so we cannot use a regular
        blocking [`stop()`][pykka.ActorRef.stop] (it would round-trip a
        message through the supervisor's own inbox). Instead we tell each
        child to stop and wait directly on the child's stopped event.
        """
        for child in reversed(list(self._super_children.values())):
            if child.dead:
                continue
            self._super_pending_stops.add(child.ref.actor_urn)
            try:
                child.ref.tell(_ActorStopMessage())
            except ActorDeadError:
                self._super_pending_stops.discard(child.ref.actor_urn)
                child.dead = True
                ActorRegistry.unregister(child.ref)
                continue
            if not self._wait_for_child_stop(child.ref.actor_urn):
                # The child did not shut down cleanly in time; it is on a
                # non-daemon thread, but we still mark it and move on so the
                # supervisor can terminate.
                logger.warning(
                    f"{self} timed out stopping child {child.spec.name!r}"
                )
            child.dead = True
            ActorRegistry.unregister(child.ref)

    def _wait_for_child_stop(self, urn: str) -> bool:
        child = self._super_by_urn[urn]
        if child.ref.actor_stopped.wait(self.stop_timeout):
            self._super_pending_stops.discard(urn)
            return True
        return False

    def _abort_children(self) -> None:
        """Stop everything that got started; never leave orphans."""
        for child in reversed(list(self._super_children.values())):
            if child.dead:
                continue
            try:
                child.ref.stop(block=True, timeout=self.stop_timeout)
            except Exception:  # noqa: BLE001
                logger.exception(
                    f"{self} failed to stop child {child.spec.name!r} "
                    "while aborting"
                )
            child.dead = True
            ActorRegistry.unregister(child.ref)

    #
    # Restart-intensity budget
    #

    def _restart_allowed(self) -> bool:
        """Record a restart attempt and report whether it is within budget."""
        now = time.monotonic()
        horizon = now - self.within_time_range
        self._super_restart_times = [
            started_at
            for started_at in self._super_restart_times
            if started_at > horizon
        ]
        if len(self._super_restart_times) >= self.max_restarts:
            return False
        self._super_restart_times.append(now)
        return True

    def _give_up(
        self,
        child: _Child,
        exc_info: OptExcInfo | None,
    ) -> None:
        child.dead = True
        ActorRegistry.unregister(child.ref)
        logger.error(
            f"{self} giving up on child {child.spec.name!r}: "
            f"restart intensity of {self.max_restarts} restarts within "
            f"{self.within_time_range}s reached"
        )
        try:
            self.on_restart_intensity_reached(child.spec, exc_info)
        except Exception:  # noqa: BLE001
            logger.exception(
                f"{self}.on_restart_intensity_reached() raised an exception"
            )

    #
    # Introspection / validation
    #

    def child_ref(self, name: str) -> ActorRef[Any] | None:
        """Get the stable reference of a child by name."""
        child = self._super_children.get(name)
        return None if child is None else child.ref

    def child_refs(self) -> dict[str, ActorRef[Any]]:
        """Get a mapping of child names to their stable references."""
        return {
            name: child.ref
            for name, child in self._super_children.items()
            if not child.dead
        }

    def _validate_specs(self, specs: list[ChildSpec]) -> None:
        used: set[str] = set()
        counters: dict[str, itertools.count[int]] = {}
        for index, spec in enumerate(specs):
            name = spec.name
            if name is None:
                base = spec.actor_class.__name__
                count = counters.setdefault(base, itertools.count(1))
                candidate = base
                while candidate in used:
                    candidate = f"{base}{next(count)}"
                name = candidate
                specs[index] = ChildSpec(
                    actor_class=spec.actor_class,
                    args=spec.args,
                    kwargs=spec.kwargs,
                    name=name,
                    start_timeout=spec.start_timeout,
                )
            if name in used:
                msg = f"Duplicate child name in supervisor: {name!r}"
                raise ValueError(msg)
            used.add(name)
        if self.max_restarts < 0:
            msg = "max_restarts must be >= 0"
            raise ValueError(msg)
        if self.within_time_range <= 0:
            msg = "within_time_range must be > 0"
            raise ValueError(msg)
