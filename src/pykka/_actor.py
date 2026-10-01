from __future__ import annotations

import abc
import logging
import sys
import threading
import uuid
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pykka import ActorDeadError, ActorRef, ActorRegistry, messages
from pykka._introspection import get_attr_directly

if TYPE_CHECKING:
    from types import TracebackType

    from pykka import Future
    from pykka._envelope import Envelope
    from pykka._types import OptExcInfo

__all__ = ["Actor"]


logger = logging.getLogger("pykka")


A = TypeVar("A", bound="Actor")


class ActorInbox(Protocol):
    def put(self, envelope: Envelope[Any], /) -> None: ...

    def get(self) -> Envelope[Any]: ...

    def empty(self) -> bool: ...


class _StartBarrier:
    """One-shot synchronization for a supervised actor's on_start() result."""

    __slots__ = ("event", "exc_info", "done")

    def __init__(self) -> None:
        self.event: threading.Event = threading.Event()
        self.exc_info: OptExcInfo | None = None
        self.done: bool = False

    def wait(self, timeout: float | None) -> bool:
        return self.event.wait(timeout)

    def signal(self, exc_info: OptExcInfo | None) -> None:
        self.exc_info = exc_info
        self.done = True
        self.event.set()


class Actor(abc.ABC):
    """An actor is an execution unit that executes concurrently with other actors.

    To create an actor:

    1.  subclass one of the [`Actor`][pykka.Actor] implementations:

        - [`ThreadingActor`][pykka.ThreadingActor]

    2.  implement your methods, including `__init__()`, as usual,
    3.  call [`Actor.start()`][pykka.Actor.start] on your actor class,
        passing the method any arguments for your constructor.

    To stop an actor, call [`Actor.stop()`][pykka.Actor.stop] or
    [`ActorRef.stop()`][pykka.ActorRef.stop].

    Example:
        ```py
        import pykka

        class MyActor(pykka.ThreadingActor):
            def __init__(self, my_arg=None):
                super().__init__()
                ... # My optional init code with access to start() arguments

            def on_start(self):
                ... # My optional setup code in same context as on_receive()

            def on_stop(self):
                ... # My optional cleanup code in same context as on_receive()

            def on_failure(self, exception_type, exception_value, traceback):
                ... # My optional cleanup code in same context as on_receive()

            def on_receive(self, message):
                ... # My optional message handling code for a plain actor

            def a_method(self, ...):
                ... # My regular method to be used through an ActorProxy

        my_actor_ref = MyActor.start(my_arg=...)
        my_actor_ref.stop()
        ```

    """

    @classmethod
    def start(
        cls: type[A],
        *args: Any,
        **kwargs: Any,
    ) -> ActorRef[A]:
        """Start an actor.

        Starting an actor also registers it in the
        [`ActorRegistry`][pykka.ActorRegistry].

        Any arguments passed to [`start()`][pykka.Actor.start] will be passed on
        to the class constructor.

        Behind the scenes, the following is happening when you call
        [`start()`][pykka.Actor.start]:

        1.  The actor is created:

            1.  [`actor_urn`][pykka.Actor.actor_urn] is initialized with the
                assigned URN.
            2.  [`actor_inbox`][pykka.Actor.actor_inbox] is initialized with a
                new actor inbox.
            3.  [`actor_ref`][pykka.Actor.actor_ref] is initialized with a
                [`pykka.ActorRef`][pykka.ActorRef] object for safely
                communicating with the actor.
            4.  At this point, your `__init__()` code can run.

        2.  The actor is registered in [`pykka.ActorRegistry`][pykka.ActorRegistry].

        3.  The actor's receive loop is started.

        Returns:
            A ref which can be used to access the actor in a safe manner.

        """
        obj = cls(*args, **kwargs)
        obj._before_loop_start()
        return cls._start_incarnation(obj, obj.actor_ref, register=True)

    @classmethod
    def _start_incarnation(
        cls: type[A],
        obj: A,
        stable_ref: ActorRef[A],
        *,
        register: bool,
    ) -> ActorRef[A]:
        """Register, start the loop, and return the ref for an incarnation.

        The first incarnation is registered before its loop starts, preserving
        the historical behavior of [`start()`][pykka.Actor.start]. Later
        incarnations of a supervised actor are already registered under their
        stable reference, so they are only retargeted.
        """
        assert stable_ref is not None, (
            "Actor.__init__() have not been called. "
            "Did you forget to call super() in your override?"
        )
        if register:
            ActorRegistry.register(stable_ref)
        else:
            stable_ref._retarget(obj)  # noqa: SLF001
        logger.debug(f"Starting {obj}")
        obj._start_actor_loop()
        return stable_ref

    @staticmethod
    @abc.abstractmethod
    def _create_actor_inbox() -> ActorInbox:
        """Create an inbox for the actor.

        Internal method for implementors of new actor types.
        """
        msg = "Use a subclass of Actor"
        raise NotImplementedError(msg)

    @staticmethod
    @abc.abstractmethod
    def _create_future() -> Future[Any]:
        """Create a future for the actor.

        Internal method for implementors of new actor types.
        """
        msg = "Use a subclass of Actor"
        raise NotImplementedError(msg)

    @abc.abstractmethod
    def _start_actor_loop(self) -> None:
        """Create and start the actor's event loop.

        Internal method for implementors of new actor types.
        """
        msg = "Use a subclass of Actor"
        raise NotImplementedError(msg)

    actor_urn: str
    """
    The actor URN string is a universally unique identifier for the actor.

    It may be used for looking up a specific actor using
    [`ActorRegistry.get_by_urn()`][pykka.ActorRegistry.get_by_urn].
    """

    actor_inbox: ActorInbox
    """
    The actor's inbox.

    Use [`ActorRef.tell()`][pykka.ActorRef.tell],
    [`ActorRef.ask()`][pykka.ActorRef.ask], and friends to put messages in the
    inbox.
    """

    _actor_ref: ActorRef[Any]

    @property
    def actor_ref(self: A) -> ActorRef[A]:
        """The actor's [`ActorRef`][pykka.ActorRef] instance."""
        # This property only exists to improve the typing of the ActorRef.
        return self._actor_ref

    actor_stopped: threading.Event
    """
    A [`threading.Event`][threading.Event] representing whether or not the actor
    should continue processing messages.

    Use [`stop()`][pykka.Actor.stop] to change it.
    """

    def __init__(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        """Create actor.

        Your are free to override `__init__()`, but you must call your
        superclass' `__init__()` to ensure that the fields
        [`actor_urn`][pykka.Actor.actor_urn],
        [`actor_inbox`][pykka.Actor.actor_inbox], and
        [`actor_ref`][pykka.Actor.actor_ref] are initialized.

        You can use `super()`:

            super().__init__()

        Or call you superclass directly:

            pykka.ThreadingActor.__init__(self)

        `__init__()` is called before the actor is started and registered
        in the [`ActorRegistry`][pykka.ActorRegistry].
        """
        self.actor_urn = uuid.uuid4().urn
        self.actor_inbox = self._create_actor_inbox()
        self.actor_stopped = threading.Event()

        self._actor_ref = ActorRef(self)
        self._supervisor_ref: ActorRef[Any] | None = None
        self._incarnation = 0
        self._start_barrier: _StartBarrier | None = None

    def _bind_to_supervision(
        self,
        *,
        supervisor_ref: ActorRef[Any],
        stable_ref: ActorRef[Any],
        stable_urn: str,
        incarnation: int,
    ) -> None:
        """Attach this incarnation to its supervisor's stable reference.

        Called on an actor instance, before its loop is started, by the
        supervision machinery.
        """
        self._supervisor_ref = supervisor_ref
        self._incarnation = incarnation
        self.actor_urn = stable_urn
        self._actor_ref = stable_ref
        self._start_barrier = _StartBarrier()

    def _before_loop_start(self) -> None:
        """Hook run after construction, before the actor loop is started.

        Ordinary actors have nothing to pin; a supervisor uses this to fix its
        stable identity before any child can observe it.
        """

    def _wait_until_started(self, timeout: float | None) -> None:
        """Block until a supervised incarnation reports on_start() outcome."""
        barrier = self._start_barrier
        assert barrier is not None

        def _consume() -> None:
            if barrier.exc_info is not None:
                _exc_type, exc_value, exc_tb = barrier.exc_info
                assert exc_value is not None
                raise exc_value.with_traceback(exc_tb)

        if not barrier.done:
            if not barrier.wait(timeout):
                msg = f"{self} did not start within {timeout} seconds"
                raise TimeoutError(msg)
        _consume()

    def _signal_started(self, exc_info: OptExcInfo | None) -> None:
        barrier = self._start_barrier
        if barrier is not None:
            barrier.signal(exc_info)

    def __str__(self) -> str:
        return f"{self.__class__.__name__} ({self.actor_urn})"

    def stop(self) -> None:
        """Stop the actor.

        It's equivalent to calling
        [`ActorRef.stop(block=False)`][pykka.ActorRef.stop].
        """
        self.actor_ref.tell(messages._ActorStop())  # noqa: SLF001

    def _stop(self) -> None:
        """Stop the actor immediately without processing the rest of the inbox."""
        if self._supervisor_ref is None:
            ActorRegistry.unregister(self.actor_ref)
        self.actor_stopped.set()
        logger.debug(f"Stopped {self}")
        try:
            self.on_stop()
        except Exception:  # noqa: BLE001
            self._handle_failure(*sys.exc_info())
        if self._supervisor_ref is not None:
            self._notify_terminated(failed=False)

    def _notify_terminated(self, *, failed: bool) -> None:
        """Inform the supervisor that this incarnation terminated.

        Sent from the actor's own thread and processed in the supervisor's own
        thread, so supervision state needs no locking. The incarnation counter
        lets the supervisor ignore stale notifications after a restart.
        """
        from pykka._supervision import _ChildTerminated  # noqa: PLC0415

        supervisor_ref = self._supervisor_ref
        assert supervisor_ref is not None
        supervisor_ref.tell(
            _ChildTerminated(
                urn=self.actor_urn,
                incarnation=self._incarnation,
                failed=failed,
            )
        )

    def _actor_loop(self) -> None:
        """Run the actor's core loop.

        This is the method that will be executed by the thread or greenlet.
        """
        self._actor_loop_setup()
        self._actor_loop_running()
        self._actor_loop_teardown()

    def _actor_loop_setup(self) -> None:
        try:
            self.on_start()
        except Exception:  # noqa: BLE001
            if not self.actor_stopped.is_set():
                self._handle_failure(*sys.exc_info())
            self._signal_started(sys.exc_info())
            if self._supervisor_ref is not None:
                self._notify_terminated(failed=True)
        else:
            self._signal_started(None)

    def _actor_loop_running(self) -> None:
        while not self.actor_stopped.is_set():
            envelope = self.actor_inbox.get()
            try:
                response = self._handle_receive(envelope.message)
                if envelope.reply_to is not None:
                    envelope.reply_to.set(response)
            except Exception:
                if envelope.reply_to is not None:
                    logger.info(
                        f"Exception returned from {self} to caller:",
                        exc_info=sys.exc_info(),
                    )
                    envelope.reply_to.set_exception()
                else:
                    self._handle_failure(*sys.exc_info())
                    try:
                        self.on_failure(*sys.exc_info())
                    except Exception:  # noqa: BLE001
                        self._handle_failure(*sys.exc_info())
                    if self._supervisor_ref is not None:
                        self._notify_terminated(failed=True)
            except BaseException:  # noqa: BLE001
                exception_value = sys.exc_info()[1]
                logger.debug(f"{exception_value!r} in {self}. Stopping all actors.")
                self._stop()
                ActorRegistry.stop_all()

    def _actor_loop_teardown(self) -> None:
        while not self.actor_inbox.empty():
            envelope = self.actor_inbox.get()
            if envelope.reply_to is not None:
                if isinstance(envelope.message, messages._ActorStop):  # noqa: SLF001
                    envelope.reply_to.set(None)
                else:
                    envelope.reply_to.set_exception(
                        exc_info=(
                            ActorDeadError,
                            ActorDeadError(
                                f"{self.actor_ref} stopped before handling the message"
                            ),
                            None,
                        )
                    )

    def on_start(self) -> None:  # noqa: B027
        """Run code at the beginning of the actor's life.

        Hook for doing any setup that should be done *after* the actor is
        started, but *before* it starts processing messages.

        /// note | Threading runtime
        For [`ThreadingActor`][pykka.ThreadingActor], this method is
        executed in the actor's own thread, while `__init__()` is executed
        in the thread that created the actor.
        ///

        If an exception is raised by this method the stack trace will be
        logged, and the actor will stop.
        """

    def on_stop(self) -> None:  # noqa: B027
        """Run code at the end of the actor's life.

        Hook for doing any cleanup that should be done *after* the actor has
        processed the last message, and *before* the actor stops.

        This hook is *not* called when the actor stops because of an unhandled
        exception. In that case, the [`on_failure()`][pykka.Actor.on_failure]
        hook is called instead.

        /// note | Threading runtime
        For [`ThreadingActor`][pykka.ThreadingActor] this method is executed
        in the actor's own thread, immediately before the thread exits.
        ///

        If an exception is raised by this method the stack trace will be
        logged, and the actor will stop.
        """

    def _handle_failure(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Log unexpected failures and stop the actor.

        A supervised actor leaves registry bookkeeping to its supervisor,
        which owns the stable reference and decides whether to restart.
        """
        logger.error(
            f"Unhandled exception in {self}:",
            exc_info=(exception_type, exception_value, traceback),  # type: ignore[arg-type]   # ty: ignore[invalid-argument-type]
        )
        if self._supervisor_ref is None:
            ActorRegistry.unregister(self.actor_ref)
        self.actor_stopped.set()

    def on_failure(  # noqa: B027
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Run code when an unhandled exception is raised.

        Hook for doing any cleanup *after* an unhandled exception is raised,
        and *before* the actor stops.

        /// note | Threading runtime
        For [`ThreadingActor`][pykka.ThreadingActor] this method is executed in
        the actor's own thread, immediately before the thread exits.
        ///

        The method's arguments are the relevant information from
        [`sys.exc_info()`][sys.exc_info].

        If an exception is raised by this method the stack trace will be
        logged, and the actor will stop.
        """

    def _handle_receive(self, message: Any) -> Any:
        """Handle messages sent to the actor."""
        if isinstance(message, messages._ActorStop):  # noqa: SLF001
            return self._stop()
        if isinstance(message, messages.ProxyCall):
            callee = get_attr_directly(self, message.attr_path)
            return callee(*message.args, **message.kwargs)
        if isinstance(message, messages.ProxyGetAttr):
            attr = get_attr_directly(self, message.attr_path)
            return attr
        if isinstance(message, messages.ProxySetAttr):
            parent_attr = get_attr_directly(self, message.attr_path[:-1])
            attr_name = message.attr_path[-1]
            return setattr(parent_attr, attr_name, message.value)
        return self.on_receive(message)

    def on_receive(self, message: Any) -> Any:
        """May be implemented for the actor to handle regular non-proxy messages.

        Args:
            message: the message to handle

        Returns:
            anything that should be sent as a reply to the sender

        """
        logger.warning(f"Unexpected message received by {self}: {message}")
