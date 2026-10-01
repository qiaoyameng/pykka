from __future__ import annotations

import threading
import weakref
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
    overload,
)

from pykka import ActorDeadError, ActorProxy
from pykka._envelope import Envelope
from pykka.messages import _ActorStop

if TYPE_CHECKING:
    from threading import Event

    from pykka import Actor, Future
    from pykka._actor import ActorInbox

__all__ = ["ActorRef"]


A = TypeVar("A", bound="Actor")


class ActorRef(Generic[A]):
    """Reference to a running actor which may safely be passed around.

    [`ActorRef`][pykka.ActorRef] instances are returned by
    [`Actor.start()`][pykka.Actor.start] and the lookup methods in
    [`ActorRegistry`][pykka.ActorRegistry]. You should never need to create
    [`ActorRef`][pykka.ActorRef] instances yourself.
    """

    actor_class: type[A]
    """The class of the referenced actor."""

    actor_urn: str
    """See [`Actor.actor_urn`][pykka.Actor.actor_urn]."""

    _lock: threading.RLock
    """Lock protecting the inbox/stopped-flag while a supervised actor restarts."""

    actor_inbox: ActorInbox
    """The inbox of the actor's current incarnation.

    For a supervised actor this follows the newest incarnation after a
    restart; the [`ActorRef`][pykka.ActorRef] object itself stays the same.
    """

    actor_stopped: Event
    """A [`threading.Event`][threading.Event] representing the ref's liveness.

    For a standalone actor this is the actor's own stopped flag. For a
    supervised actor it is only set when the actor is permanently stopped,
    not when an individual incarnation is restarted.
    """

    _stop_requested: bool
    """Whether a graceful stop has been requested through this ref."""

    _incarnation_ended: Event
    """Set when the current incarnation's thread has fully finished."""

    def __init__(
        self,
        actor: A,
    ) -> None:
        self._actor_weakref = weakref.ref(actor)
        self._lock = threading.RLock()
        self.actor_class = actor.__class__
        self.actor_urn = actor.actor_urn
        self.actor_inbox = actor.actor_inbox
        self.actor_stopped = actor.actor_stopped
        self._stop_requested = False
        self._incarnation_ended = actor._incarnation_ended

    def _bind_supervised(self, actor: A) -> None:
        """Make the ref represent a supervised actor's first incarnation."""
        permanent_stopped = threading.Event()
        with self._lock:
            self._actor_weakref = weakref.ref(actor)
            self.actor_class = actor.__class__
            self.actor_urn = actor.actor_urn
            self.actor_inbox = actor.actor_inbox
            self.actor_stopped = permanent_stopped
            self._incarnation_ended = actor._incarnation_ended

    def _rebind(self, actor: A) -> tuple[ActorInbox, bool]:
        """Point the ref at a new incarnation.

        The URN and permanent stopped event are kept stable. Returns the
        previous inbox and whether a graceful stop had already been requested.
        """
        with self._lock:
            previous_inbox = self.actor_inbox
            stop_requested = self._stop_requested
            self._actor_weakref = weakref.ref(actor)
            self.actor_class = actor.__class__
            self.actor_inbox = actor.actor_inbox
            return previous_inbox, stop_requested

    def _mark_permanently_stopped(self) -> None:
        """Mark the ref as permanently dead, independent of any incarnation."""
        with self._lock:
            self.actor_stopped.set()

    def __repr__(self) -> str:
        return f"<ActorRef for {self}>"

    def __str__(self) -> str:
        return f"{self.actor_class.__name__} ({self.actor_urn})"

    def is_alive(self) -> bool:
        """Check if actor is alive.

        This is based on the actor's stopped flag. The actor is not guaranteed
        to be alive and responding even though
        [`is_alive()`][pykka.ActorRef.is_alive] returns `True`.

        Returns:
            `True` if actor is alive, `False` otherwise.

        """
        return not self.actor_stopped.is_set()

    def tell(
        self,
        message: Any,
    ) -> None:
        """Send message to actor without waiting for any response.

        Will generally not block, but if the underlying queue is full it will
        block until a free slot is available.

        Args:
            message: message to send

        Raises:
            ActorDeadError: if actor is not available

        """
        with self._lock:
            if not self.is_alive():
                msg = f"{self} not found"
                raise ActorDeadError(msg)
            self.actor_inbox.put(Envelope(message))

    @overload
    def ask(
        self,
        message: Any,
        *,
        block: Literal[False],
        timeout: float | None = None,
    ) -> Future[Any]: ...

    @overload
    def ask(
        self,
        message: Any,
        *,
        block: Literal[True],
        timeout: float | None = None,
    ) -> Any: ...

    @overload
    def ask(
        self,
        message: Any,
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> Any | Future[Any]: ...

    def ask(
        self,
        message: Any,
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> Any | Future[Any]:
        """Send message to actor and wait for the reply.

        The message can be of any type.
        If `block` is `False`, it will immediately return a
        [`Future`][pykka.Future] instead of blocking.

        If `block` is `True`, and `timeout` is `None`, as default, the method
        will block until it gets a reply, potentially forever. If `timeout` is
        an integer or float, the method will wait for a reply for `timeout`
        seconds, and then raise [`pykka.Timeout`][pykka.Timeout].

        Args:
            message: message to send
            block: whether to block while waiting for a reply
            timeout: seconds to wait before timeout if blocking

        Raises:
            Timeout: if timeout is reached if blocking
            Exception: any exception returned by the receiving actor if blocking

        Returns:
            a future if not blocking, or a response if blocking

        """
        future = self.actor_class._create_future()  # noqa: SLF001

        with self._lock:
            try:
                if not self.is_alive():
                    msg = f"{self} not found"
                    raise ActorDeadError(msg)  # noqa: TRY301
            except ActorDeadError:
                future.set_exception()
            else:
                self.actor_inbox.put(Envelope(message, reply_to=future))

        if block:
            return future.get(timeout=timeout)

        return future

    @overload
    def stop(
        self,
        *,
        block: Literal[True],
        timeout: float | None = None,
    ) -> bool: ...

    @overload
    def stop(
        self,
        *,
        block: Literal[False],
        timeout: float | None = None,
    ) -> Future[bool]: ...

    @overload
    def stop(
        self,
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> bool | Future[bool]: ...

    def stop(
        self,
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> bool | Future[bool]:
        """Send a message to the actor, asking it to stop.

        Returns `True` if actor is stopped or was being stopped at the
        time of the call. `False` if actor was already dead. If
        `block` is `False`, it returns a future wrapping the result.

        Messages sent to the actor before the actor is asked to stop will
        be processed normally before it stops.

        Messages sent to the actor after the actor is asked to stop will
        be replied to with [`ActorDeadError`][pykka.ActorDeadError] after it
        stops.

        The actor may not be restarted.

        `block` and `timeout` works as for [`ask()`][pykka.ActorRef.ask].

        Returns:
            a future if not blocking, or a boolean result if blocking

        """
        with self._lock:
            if self.is_alive():
                self._stop_requested = True
        ask_future = self.ask(_ActorStop(), block=False)

        def _stop_result_converter(timeout: float | None) -> bool:
            try:
                ask_future.get(timeout=timeout)
            except ActorDeadError:
                return False
            else:
                return True

        converted_future = ask_future.__class__()
        converted_future.set_get_hook(_stop_result_converter)
        converted_future = cast("Future[bool]", converted_future)

        if block:
            return converted_future.get(timeout=timeout)

        return converted_future

    def proxy(self: ActorRef[A]) -> ActorProxy[A]:
        """Wrap the [`ActorRef`][pykka.ActorRef] in an [`ActorProxy`][pykka.ActorProxy].

        Using this method like this:

            proxy = AnActor.start().proxy()

        is analogous to:

            proxy = ActorProxy(AnActor.start())

        Raises:
            ActorDeadError: if actor is not available

        Returns:
            a proxy object wrapping the actor reference.

        """
        return ActorProxy(actor_ref=self)
