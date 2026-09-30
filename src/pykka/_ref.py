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

from pykka import ActorDeadError, ActorProxy, MailboxFull
from pykka._envelope import Envelope

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

    actor_inbox: ActorInbox
    """See [`Actor.actor_inbox`][pykka.Actor.actor_inbox]."""

    actor_stopped: Event
    """See [`Actor.actor_stopped`][pykka.Actor.actor_stopped]."""

    def __init__(
        self,
        actor: A,
    ) -> None:
        self._actor_weakref = weakref.ref(actor)
        self.actor_class = actor.__class__
        self.actor_urn = actor.actor_urn
        self.actor_inbox = actor.actor_inbox
        self.actor_stopped = actor.actor_stopped

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
        *,
        timeout: float | None = 0,
    ) -> None:
        """Send message to actor without waiting for any response.

        If the actor's mailbox is unbounded, or has free capacity, the
        message is enqueued immediately.

        If the mailbox has a finite capacity and is full, the call waits up
        to `timeout` seconds for free capacity. With the default `timeout` of
        `0`, [`MailboxFull`][pykka.MailboxFull] is raised immediately instead.
        Pass `None` to wait until capacity is available.

        A message is never lost: either it is successfully enqueued, or an
        exception is raised.

        Args:
            message: message to send
            timeout: seconds to wait for mailbox capacity; `0` to fail
                immediately when full, `None` to wait forever

        Raises:
            ActorDeadError: if actor is not available
            MailboxFull: if a bounded mailbox is full and no capacity became
                available in time

        """
        if self._is_self_send():
            timeout = 0

        if not self.is_alive():
            msg = f"{self} not found"
            raise ActorDeadError(msg)
        self.actor_inbox.put(Envelope(message), timeout)

    def _is_self_send(self) -> bool:
        """Return `True` if called from the actor's own consumer thread.

        The actor is the mailbox's only consumer. A blocking enqueue from the
        consumer thread of a full mailbox would deadlock, since the actor
        would be waiting for itself to make room. Such sends are therefore
        always made non-blocking and fail fast with
        [`MailboxFull`][pykka.MailboxFull].
        """
        consumer_ident = getattr(self.actor_inbox, "consumer_ident", None)
        return consumer_ident is not None and threading.get_ident() == consumer_ident

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

        If the actor uses a bounded mailbox that is full, the method waits up
        to `timeout` seconds for free capacity too. When `block` is `True`,
        the `timeout` covers both waiting to enqueue and waiting for the
        reply. If capacity never becomes available, a
        [`MailboxFull`][pykka.MailboxFull] is raised, or set on the returned
        future when `block` is `False`.

        Args:
            message: message to send
            block: whether to block while waiting for a reply
            timeout: seconds to wait for enqueue and/or the reply if blocking;
                for non-blocking calls, seconds to wait for mailbox capacity

        Raises:
            Timeout: if timeout is reached if blocking
            MailboxFull: if a bounded mailbox is full and no capacity became
                available in time
            Exception: any exception returned by the receiving actor if blocking

        Returns:
            a future if not blocking, or a response if blocking

        """
        future = self.actor_class._create_future()  # noqa: SLF001

        enqueue_timeout = 0.0 if self._is_self_send() else timeout

        try:
            if not self.is_alive():
                msg = f"{self} not found"
                raise ActorDeadError(msg)  # noqa: TRY301
            self.actor_inbox.put(
                Envelope(message, reply_to=future),
                enqueue_timeout,
            )
        except ActorDeadError:
            future.set_exception()
        except MailboxFull:
            future.set_exception(
                exc_info=(MailboxFull, MailboxFull("Actor mailbox is full"), None)
            )

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

        The stop signal is delivered through a channel independent of the
        message mailbox, so a full bounded mailbox cannot delay or prevent
        the actor from stopping.

        Messages sent to the actor before it is asked to stop and
        successfully enqueued are processed normally before it stops. New
        messages arriving after the stop request are replied to with
        [`ActorDeadError`][pykka.ActorDeadError].

        The actor may not be restarted.

        `block` and `timeout` works as for [`ask()`][pykka.ActorRef.ask].

        Returns:
            a future if not blocking, or a boolean result if blocking

        """
        ask_future = self.actor_class._create_future()  # noqa: SLF001

        if not self.is_alive():
            ask_future.set_exception(
                exc_info=(
                    ActorDeadError,
                    ActorDeadError(f"{self} not found"),
                    None,
                )
            )
        else:
            self.actor_inbox.request_stop(ask_future)

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
