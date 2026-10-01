"""Supervision trees for actors.

A supervisor is an actor that declares the actors it is responsible for. The
supervisor starts its children, monitors their actual life cycle, and restarts
them according to a restart strategy when they crash.

Only ordinary lifecycle failures trigger a restart: an exception returned to
the caller of an `ask()` does not, as the actor stays alive.
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from pykka import ActorDeadError, messages
from pykka._envelope import Envelope
from pykka._registry import ActorRegistry

if TYPE_CHECKING:
    from pykka import Actor, ActorRef

__all__ = [
    "Child",
    "RestartStrategy",
    "Supervisor",
    "SupervisorStrategy",
]


logger = logging.getLogger("pykka")


class RestartStrategy(enum.Enum):
    """Strategy deciding which children are affected when one child fails."""

    ONE_FOR_ONE = "one_for_one"
    """Only the child that failed is restarted."""

    ALL_FOR_ONE = "all_for_one"
    """All children are restarted when any child fails."""


@dataclass(frozen=True)
class SupervisorStrategy:
    """Configuration for a supervisor's restart behavior.

    If a child is restarted more than `max_restarts` times within
    `within_seconds` seconds, the child is stopped permanently instead of
    being restarted again. The window slides: failures older than
    `within_seconds` do not count.

    Args:
        max_restarts: maximum number of restarts allowed inside the window,
            or `None` for an unlimited number of restarts.
        within_seconds: length of the restart budget window in seconds, or
            `None` to never expire old restarts.
        strategy: the restart strategy to apply.
    """

    max_restarts: int = 3
    within_seconds: float | None = 5.0
    strategy: RestartStrategy = RestartStrategy.ONE_FOR_ONE


@dataclass(frozen=True)
class Child:
    """Declaration of a supervised child actor.

    Args:
        actor_class: the actor class to start.
        args: positional arguments to pass to the actor's constructor.
        kwargs: keyword arguments to pass to the actor's constructor.
        name: optional human-readable name used to identify the child.
        strategy: optional per-child strategy, overriding the supervisor's
            default. Only the restart budget is per child.
    """

    actor_class: type[Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    strategy: SupervisorStrategy | None = None


class _ChildEntry:
    """Supervisor-internal bookkeeping for one supervised child."""

    def __init__(
        self,
        spec: Child,
        name: str,
        default_strategy: SupervisorStrategy,
    ) -> None:
        self.spec = spec
        self.name = name
        self.strategy = spec.strategy or default_strategy
        self.ref: ActorRef[Any] | None = None
        self.restart_times: deque[float] = deque()
        self.last_failure: BaseException | None = None
        self.incarnation_dead = threading.Event()
        self.stashed_envelopes: list[Envelope[Any]] = []
        self.supervisor_ref: ActorRef[Any] | None = None
        self.generation = 0
        self.given_up = False

    @property
    def effective_strategy(self) -> SupervisorStrategy:
        return self.strategy


def _drain_inbox(inbox: Any, bucket: list[Envelope[Any]]) -> None:
    while True:
        try:
            bucket.append(inbox.get_nowait())
        except Exception:  # noqa: BLE001
            return


def _reply_undelivered(envelopes: list[Envelope[Any]]) -> None:
    for envelope in envelopes:
        if envelope.reply_to is None:
            continue
        if isinstance(envelope.message, messages._ActorStop):
            envelope.reply_to.set(None)
        else:
            envelope.reply_to.set_exception(
                exc_info=(
                    ActorDeadError,
                    ActorDeadError("Supervised actor stopped permanently"),
                    None,
                )
            )


class _SupervisorRuntime:
    """The mutable supervision state owned by a supervisor actor instance."""

    def __init__(
        self,
        actor: Actor,
        children: tuple[Child, ...],
        strategy: SupervisorStrategy,
    ) -> None:
        self.actor = actor
        self.stopping = False
        entries: list[_ChildEntry] = []
        used_names: set[str] = set()
        for index, spec in enumerate(children):
            base_name = spec.name or f"child-{index}-{spec.actor_class.__name__}"
            name = base_name
            suffix = 1
            while name in used_names:
                suffix += 1
                name = f"{base_name}-{suffix}"
            used_names.add(name)
            entries.append(_ChildEntry(spec, name, strategy))
        self.entries = entries
        self.by_name = {entry.name: entry for entry in entries}

    def start_all(self) -> None:
        """Start all children, in declaration order.

        If a child's constructor or initial start fails, the children that
        were already started are stopped in reverse order so that no orphan
        actors are left behind.
        """
        started: list[_ChildEntry] = []
        for entry in self.entries:
            try:
                entry.ref = entry.spec.actor_class._start_supervised(  # noqa: SLF001
                    self.actor,
                    entry,
                    *entry.spec.args,
                    **entry.spec.kwargs,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    f"Supervised child {entry.name} failed to start; "
                    "stopping already started children"
                )
                self.stopping = True
                for prior in reversed(started):
                    self._terminate_entry(prior)
                raise
            started.append(entry)

    def stop_all(self) -> None:
        """Stop all children, in reverse declaration order."""
        self.stopping = True
        for entry in reversed(self.entries):
            self._terminate_entry(entry)

    def on_child_terminated(
        self,
        name: str,
        failed: bool,
        generation: int,
    ) -> None:
        """Handle a child incarnation life-cycle notification."""
        entry = self.by_name.get(name)
        if entry is None:
            return
        if generation != entry.generation:
            return
        entry.incarnation_dead.set()
        if self.stopping:
            # The supervisor is shutting down: never restart, and make sure
            # the newest incarnation is actually terminated even if a stop
            # message is already queued in its inbox.
            self._terminate_entry(entry)
            return
        if not failed:
            self._give_up(entry, already_dead=True)
            return
        self._handle_failure(entry)

    def _handle_failure(self, entry: _ChildEntry) -> None:
        strategy = entry.effective_strategy
        if not self._within_restart_budget(entry, strategy):
            logger.error(
                f"Supervised child {entry.name} exceeded "
                f"{strategy.max_restarts} restarts in "
                f"{strategy.within_seconds} seconds; stopping permanently"
            )
            self._give_up(entry, already_dead=True)
            return
        logger.info(
            f"Restarting supervised child {entry.name} "
            f"(restart {len(entry.restart_times)})"
        )
        try:
            if strategy.strategy is RestartStrategy.ALL_FOR_ONE:
                self._restart_all(entry)
            else:
                self._reincarnate(entry, expect_dead=True)
        except Exception:  # noqa: BLE001
            logger.exception(
                f"Failed to restart supervised child {entry.name}; "
                "stopping permanently"
            )
            self._give_up(entry, already_dead=False)

    def _within_restart_budget(
        self,
        entry: _ChildEntry,
        strategy: SupervisorStrategy,
    ) -> bool:
        if strategy.max_restarts is None:
            entry.restart_times.append(time.monotonic())
            return True
        now = time.monotonic()
        if strategy.within_seconds is not None:
            horizon = now - strategy.within_seconds
            while entry.restart_times and entry.restart_times[0] < horizon:
                entry.restart_times.popleft()
        if len(entry.restart_times) >= strategy.max_restarts:
            return False
        entry.restart_times.append(now)
        return True

    def _restart_all(self, failed_entry: _ChildEntry) -> None:
        # The failed child gets a new incarnation immediately; its siblings
        # are restarted gracefully one by one.
        self._reincarnate(failed_entry, expect_dead=True)
        for entry in self.entries:
            if entry is failed_entry:
                continue
            self._restart_healthy(entry)

    def _restart_healthy(self, entry: _ChildEntry) -> None:
        ref = entry.ref
        if ref is None:
            return
        expected_generation = entry.generation
        try:
            ref.tell(messages._SupervisedRestart())  # noqa: SLF001
        except ActorDeadError:
            pass
        incarnation_ended = ref._incarnation_ended  # noqa: SLF001
        if incarnation_ended.wait(10):
            if entry.generation != expected_generation:
                return
            # The incarnation died unexpectedly (e.g. on_stop failed) and no
            # decision is pending; replace it directly.
            self._reincarnate(entry, expect_dead=False)
        else:
            msg = f"Supervised child {entry.name} did not restart in time"
            raise TimeoutError(msg)

    def _reincarnate(
        self,
        entry: _ChildEntry,
        *,
        expect_dead: bool,
    ) -> None:
        ref = entry.ref
        if ref is None:
            msg = f"Supervised child {entry.name} was never started"
            raise RuntimeError(msg)
        if expect_dead and not entry.incarnation_dead.wait(10):
            msg = f"Supervised child {entry.name} did not stop in time"
            raise TimeoutError(msg)

        entry.generation += 1
        entry.incarnation_dead.clear()
        entry.stashed_envelopes = []
        new_actor = entry.spec.actor_class(
            *entry.spec.args,
            **entry.spec.kwargs,
        )
        new_actor.actor_urn = ref.actor_urn
        with ref._lock:  # noqa: SLF001
            old_inbox, stop_requested = ref._rebind(new_actor)  # noqa: SLF001
            carried: list[Envelope[Any]] = []
            _drain_inbox(old_inbox, carried)
            carried = entry.stashed_envelopes + carried
            new_actor._supervisor_entry = entry  # noqa: SLF001
            new_actor._actor_ref = ref  # noqa: SLF001
            new_actor._failed = False  # noqa: SLF001
            new_actor._restarting = False  # noqa: SLF001
            ref._incarnation_ended = new_actor._incarnation_ended  # noqa: SLF001
            for envelope in carried:
                new_actor.actor_inbox.put(envelope)
            new_actor._start_actor_loop()  # noqa: SLF001
        if stop_requested:
            try:
                ref.stop()
            except ActorDeadError:
                self._give_up(entry, already_dead=False)

    def _give_up(
        self,
        entry: _ChildEntry,
        *,
        already_dead: bool,
    ) -> None:
        """Permanently give up on a child."""
        if entry.ref is None:
            return
        if entry.given_up:
            return
        ref = entry.ref
        if not already_dead:
            self._terminate_entry(entry)
        with ref._lock:  # noqa: SLF001
            entry.given_up = True
            pending: list[Envelope[Any]] = []
            pending.extend(entry.stashed_envelopes)
            entry.stashed_envelopes = []
            _drain_inbox(ref.actor_inbox, pending)
            ref._mark_permanently_stopped()  # noqa: SLF001
        ActorRegistry.unregister(ref)
        self.actor.on_supervision_failure(entry.name, entry.last_failure)
        _reply_undelivered(pending)
        # If the child's very first incarnation cannot be started, stop the
        # whole supervisor so that no already-started children are left as
        # orphans.
        if entry.generation == 0 and not self.stopping:
            self.actor._stop()  # noqa: SLF001

    def _terminate_entry(self, entry: _ChildEntry) -> None:
        """Stop one child for good and wait for its incarnation to end.

        This runs in the supervisor's own thread while it is shutting down,
        so it must not wait for replies through the supervisor's inbox.
        """
        ref = entry.ref
        if ref is None:
            return
        with ref._lock:  # noqa: SLF001
            alive = ref.is_alive()
            ref._stop_requested = True  # noqa: SLF001
        already_ending = entry.incarnation_dead.is_set()
        if (
            alive
            and not already_ending
            and not ref._incarnation_ended.is_set()  # noqa: SLF001
        ):
            try:
                ref.actor_inbox.put(Envelope(messages._ActorStop()))  # noqa: SLF001
            except ActorDeadError:
                pass
        if not ref._incarnation_ended.wait(10):  # noqa: SLF001
            logger.warning(f"Supervised child {entry.name} did not stop in time")
        if not ref.actor_stopped.is_set():
            entry.given_up = True
            pending: list[Envelope[Any]] = []
            with ref._lock:
                _drain_inbox(ref.actor_inbox, pending)
                ref._mark_permanently_stopped()  # noqa: SLF001
            ActorRegistry.unregister(ref)
            _reply_undelivered(pending)


class Supervisor:
    """Mixin for actors that supervise a set of child actors.

    Example:
        ```py
        import pykka

        class Worker(pykka.ThreadingActor):
            ...

        class Manager(pykka.Supervisor, pykka.ThreadingActor):
            children = (
                pykka.Child(Worker, name="worker-1"),
                pykka.Child(Worker, name="worker-2"),
            )
            strategy = pykka.SupervisorStrategy(
                max_restarts=3,
                within_seconds=5,
                strategy=pykka.RestartStrategy.ONE_FOR_ONE,
            )
        ```

    Children are started in declaration order before the supervisor's
    [`on_start()`][pykka.Actor.on_start] hook runs and stopped in reverse
    declaration order when the supervisor stops.
    """

    children: tuple[Child, ...] = ()
    """Declared children, started in order and stopped in reverse order."""

    strategy: ClassVar[SupervisorStrategy] = SupervisorStrategy()
    """Default restart strategy for the supervisor's children."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        super().__init__(*_args, **_kwargs)
        self._supervisor_runtime = _SupervisorRuntime(
            self,
            tuple(self.children),
            self.strategy,
        )

    def on_supervision_failure(
        self,
        name: str,
        exception: BaseException | None,
    ) -> None:
        """Hook called when a child is given up permanently.

        This happens when the restart budget is exhausted or a child cannot
        be restarted. Override it to react to a permanently lost child; the
        default implementation only logs.

        Args:
            name: the name of the child that was given up.
            exception: the exception that killed the last incarnation, or
                `None` if the child stopped gracefully.
        """
        logger.error(
            f"Supervised child {name} is permanently stopped",
            exc_info=exception,
        )
