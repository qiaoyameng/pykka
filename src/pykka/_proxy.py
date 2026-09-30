from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from pykka import ActorDeadError, messages
from pykka._future import Future
from pykka._introspection import AttrInfo, introspect_attrs

if TYPE_CHECKING:
    from pykka import Actor, ActorRef
    from pykka._types import AttrPath

__all__ = ["ActorProxy", "BatchProxy"]


logger = logging.getLogger("pykka")


T = TypeVar("T")
A = TypeVar("A", bound="Actor")


class ActorProxy(Generic[A]):
    """A proxy object for an actor's attributes and methods, via an `ActorRef`.

    The proxy allows the referenced actor to be used through regular
    method calls and field access.

    # Creating a proxy

    You can create an [`ActorProxy`][pykka.ActorProxy] from any
    [`ActorRef`][pykka.ActorRef]:

        actor_ref = MyActor.start()
        actor_proxy = ActorProxy(actor_ref)

    You can also get an [`ActorProxy`][pykka.ActorProxy] by using
    [`proxy()`][pykka.ActorRef.proxy]:

        actor_proxy = MyActor.start().proxy()

    # Attributes and method calls

    When reading an attribute or getting a return value from a method, you get a
    [`Future`][pykka.Future] object back. To get the enclosed value from the
    future, you must call [`get()`][pykka.Future.get] on the returned future:

        print(actor_proxy.string_attribute.get())
        print(actor_proxy.count().get() + 1)

    If you call a method just for it's side effects and do not care about the
    return value, you do not need to accept the returned future or call
    [`get()`][pykka.Future.get] on the future. Simply call the method, and it
    will be executed concurrently with your own code:

        actor_proxy.method_with_side_effect()

    If you want to block your own code from continuing while the other method
    is processing, you can use [`get()`][pykka.Future.get] to block until it
    completes::

        actor_proxy.method_with_side_effect().get()

    You can also use the `await` keyword to block until the method completes:

        await actor_proxy.method_with_side_effect()

    If you access a proxied method as an attribute, without calling it, you
    get an [`CallableProxy`][pykka.CallableProxy].

    # Proxy to itself

    An actor can use a proxy to itself to schedule work for itself. The
    scheduled work will only be done after the current message and all messages
    already in the inbox are processed.

    For example, if an actor can split a time consuming task into multiple
    parts, and after completing each part can ask itself to start on the next
    part using proxied calls or messages to itself, it can react faster to
    other incoming messages as they will be interleaved with the parts of the
    time consuming task. This is especially useful for being able to stop the
    actor in the middle of a time consuming task.

    To create a proxy to yourself, use the actor's
    [`actor_ref`][pykka.Actor.actor_ref] attribute:

        proxy_to_myself_in_the_future = self.actor_ref.proxy()

    If you create a proxy in your actor's constructor or
    [`on_start()`][pykka.Actor.on_start] method, you can create a nice API for
    deferring work to yourself in the future:

        def __init__(self):
            ...
            self._in_future = self.actor_ref.proxy()
            ...

        def do_work(self):
            ...
            self._in_future.do_more_work()
            ...

        def do_more_work(self):
            ...

    To avoid infinite loops during proxy introspection, proxies to self
    should be kept as private instance attributes by prefixing the attribute
    name with `_`.

    Example:
        ```title="examples/cooperation.py"
        --8<-- "examples/cooperation.py"
        ```

    Args:
        actor_ref: reference to the actor to proxy

    Raises:
        pykka.ActorDeadError: if the actor is not alive

    """

    actor_ref: ActorRef[A]
    """The actor's [`pykka.ActorRef`][pykka.ActorRef] instance."""

    _actor: A
    _attr_path: AttrPath
    _known_attrs: dict[AttrPath, AttrInfo]
    _actor_proxies: dict[AttrPath, ActorProxy[A]]
    _callable_proxies: dict[AttrPath, CallableProxy[A]]

    def __init__(
        self,
        *,
        actor_ref: ActorRef[A],
        attr_path: AttrPath | None = None,
    ) -> None:
        if not actor_ref.is_alive():
            msg = f"{actor_ref} not found"
            raise ActorDeadError(msg)
        if (actor := actor_ref._actor_weakref()) is None:  # noqa: SLF001
            msg = f"{actor_ref}'s actor weakref has been deallocated"
            raise ActorDeadError(msg)
        self.actor_ref = actor_ref
        self._actor = actor
        self._attr_path = attr_path or ()
        self._known_attrs = introspect_attrs(root=self._actor, proxy=self)
        self._actor_proxies = {}
        self._callable_proxies = {}

    def __eq__(
        self,
        other: object,
    ) -> bool:
        if not isinstance(other, ActorProxy):
            return False
        if self._actor != other._actor:  # pyright: ignore[reportUnknownMemberType]
            return False
        return self._attr_path == other._attr_path

    def __hash__(self) -> int:
        return hash((self._actor, self._attr_path))

    def __repr__(self) -> str:
        return f"<ActorProxy for {self.actor_ref}, attr_path={self._attr_path!r}>"

    def __dir__(self) -> list[str]:
        result = ["__class__"]
        result += list(self.__class__.__dict__.keys())
        result += list(self.__dict__.keys())
        result += [attr_path[0] for attr_path in list(self._known_attrs.keys())]
        return sorted(result)

    def batch(self) -> BatchProxy[A]:
        """Create a batch proxy for recording operations on this actor.

        The returned [`BatchProxy`][pykka.BatchProxy] records attribute
        reads, attribute writes, and method calls without sending any
        messages. When
        [`BatchProxy.execute()`][pykka.BatchProxy.execute] is called, all
        recorded operations are sent to the actor as a single inbox message
        and executed in order, without any other messages being processed
        in between.

        Example:
            ```py
            proxy = AnActor.start().proxy()

            batch = proxy.batch()
            batch.count  # Records an attribute read
            batch.increment(5)  # Records a method call
            results = batch.execute().get()
            ```

        Returns:
            a new [`BatchProxy`][pykka.BatchProxy] for this actor

        /// note | Version added: Pykka 4.5
        ///
        """
        return BatchProxy(proxy=self)

    def __getattr__(self, name: str) -> Any:
        """Get a field or callable from the actor."""
        attr_path: AttrPath = (*self._attr_path, name)

        if attr_path not in self._known_attrs:
            self._known_attrs = introspect_attrs(root=self._actor, proxy=self)

        attr_info = self._known_attrs.get(attr_path)
        if attr_info is None:
            msg = f"{self} has no attribute {name!r}"
            raise AttributeError(msg)

        if attr_info.callable:
            if attr_path not in self._callable_proxies:
                self._callable_proxies[attr_path] = CallableProxy(
                    actor_ref=self.actor_ref,
                    attr_path=attr_path,
                )
            return self._callable_proxies[attr_path]

        if attr_info.traversable:
            if attr_path not in self._actor_proxies:
                self._actor_proxies[attr_path] = ActorProxy(
                    actor_ref=self.actor_ref,
                    attr_path=attr_path,
                )
            return self._actor_proxies[attr_path]

        message = messages.ProxyGetAttr(attr_path=attr_path)
        return self.actor_ref.ask(message, block=False)

    def __setattr__(
        self,
        name: str,
        value: Any,
    ) -> None:
        """Set a field on the actor.

        Blocks until the field is set to check if any exceptions was raised.
        """
        if name == "actor_ref" or name.startswith("_"):
            return super().__setattr__(name, value)
        attr_path = (*self._attr_path, name)
        message = messages.ProxySetAttr(attr_path=attr_path, value=value)
        self.actor_ref.ask(message)
        return None


class CallableProxy(Generic[A]):
    """Proxy to a single method.

    [`CallableProxy`][pykka.CallableProxy] instances are returned when accessing
    methods on a [`ActorProxy`][pykka.ActorProxy] without calling them.

    Example:
        ```py
        proxy = AnActor.start().proxy()

        # Ask semantics returns a future. See `__call__()` docs.
        future = proxy.do_work()

        # Tell semantics are fire and forget. See `defer()` docs.
        proxy.do_work.defer()
        ```

    """

    actor_ref: ActorRef[A]
    _attr_path: AttrPath

    def __init__(
        self,
        *,
        actor_ref: ActorRef[A],
        attr_path: AttrPath,
    ) -> None:
        self.actor_ref = actor_ref
        self._attr_path = attr_path

    def __call__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        """Call with [`ask()`][pykka.ActorRef.ask] semantics.

        Returns a future which will yield the called method's return value.

        If the call raises an exception it is set on the future, and the exception is
        reraised by [`get()`][pykka.Future.get]. If the future is left unused,
        the exception will not be reraised. Either way, the exception will
        also be logged. See the logging section in the docs for details.
        """
        message = messages.ProxyCall(
            attr_path=self._attr_path, args=args, kwargs=kwargs
        )
        return self.actor_ref.ask(message, block=False)

    def defer(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Call with [`tell()`][pykka.ActorRef.tell] semantics.

        Does not create or return a future.

        If the call raises an exception, there is no future to set the
        exception on. Thus, the actor's [`on_failure()`][pykka.Actor.on_failure]
        hook is called instead.

        /// note | Version added: Pykka 2.0
        ///
        """
        message = messages.ProxyCall(
            attr_path=self._attr_path, args=args, kwargs=kwargs
        )
        self.actor_ref.tell(message)


class BatchProxy(Generic[A]):
    """Records proxy operations and executes them as a single batch.

    A batch proxy is created with
    [`ActorProxy.batch()`][pykka.ActorProxy.batch]. It records attribute
    reads, attribute writes, and method calls without sending anything to
    the actor. When [`execute()`][pykka.BatchProxy.execute] is called, all
    recorded operations are sent to the actor as a *single* inbox message.
    The actor then executes the operations in order, and no other messages
    can be processed by the actor in between them. This makes batches
    useful for reading a consistent snapshot of several attributes, or for
    applying a group of changes atomically with respect to other messages.

    Example:
        ```py
        proxy = Counter.start().proxy()

        batch = proxy.batch()
        batch.count  # Records an attribute read
        batch.increment(5)  # Records a method call
        batch.name = "counter"  # Records an attribute write

        # Sends all three operations as one message:
        count, incremented, _ = batch.execute().get()
        ```

    Attribute paths are validated against the same rules as regular proxy
    attribute access: private attributes are not accessible, and nested
    attributes are only accessible if marked with
    [`traversable()`][pykka.traversable].

    Recording an operation does not return a future; the results of all
    operations are returned by [`execute()`][pykka.BatchProxy.execute] as
    a single future of a list, with results ordered by the position the
    operations were recorded in.

    Note that a batch is *not* a transaction: if an operation fails, the
    side effects of the operations already executed are not rolled back.

    A batch must not block waiting for another future of the same actor,
    as the actor is busy executing the batch and cannot complete the other
    future, which would deadlock. To help avoid this,
    [`Future`][pykka.Future] instances are rejected as call arguments when
    recording a method call.

    /// note | Version added: Pykka 4.5
    ///
    """

    _proxy: ActorProxy[A]
    _attr_path: AttrPath
    _operations: list[messages.ProxyOperation]

    def __init__(
        self,
        *,
        proxy: ActorProxy[A],
        attr_path: AttrPath | None = None,
        operations: list[messages.ProxyOperation] | None = None,
    ) -> None:
        self._proxy = proxy
        self._attr_path = attr_path or ()
        self._operations = operations if operations is not None else []

    def __repr__(self) -> str:
        return (
            f"<BatchProxy for {self._proxy.actor_ref}, "
            f"attr_path={self._attr_path!r}, "
            f"operations={len(self._operations)}>"
        )

    def __getattr__(self, name: str) -> Any:
        """Record a field read, or get a traversable or callable recorder."""
        attr_path: AttrPath = (*self._attr_path, name)

        known_attrs = self._proxy._known_attrs  # noqa: SLF001
        if attr_path not in known_attrs:
            known_attrs = introspect_attrs(
                root=self._proxy._actor,  # noqa: SLF001
                proxy=self._proxy,
            )
            self._proxy._known_attrs = known_attrs  # noqa: SLF001

        attr_info = known_attrs.get(attr_path)
        if attr_info is None:
            msg = f"{self._proxy} has no attribute {name!r}"
            raise AttributeError(msg)

        if attr_info.callable:
            return _BatchCallableProxy(batch=self, attr_path=attr_path)

        if attr_info.traversable:
            return BatchProxy(
                proxy=self._proxy,
                attr_path=attr_path,
                operations=self._operations,
            )

        self._operations.append(messages.ProxyGetAttr(attr_path=attr_path))
        return None

    def __setattr__(
        self,
        name: str,
        value: Any,
    ) -> None:
        """Record a field write on the actor."""
        if name.startswith("_"):
            return super().__setattr__(name, value)
        attr_path = (*self._attr_path, name)
        self._operations.append(
            messages.ProxySetAttr(attr_path=attr_path, value=value)
        )
        return None

    def execute(
        self,
        *,
        on_error: Literal["stop", "collect"] = "stop",
    ) -> Future[list[Any]]:
        """Send the recorded operations to the actor as a single message.

        The operations are executed by the actor in the order they were
        recorded, and no other messages are processed by the actor in
        between them.

        The returned future yields a list with one result per recorded
        operation, ordered by the position the operations were recorded
        in. Attribute writes yield `None`.

        If an operation raises an exception, the behavior depends on
        `on_error`:

        - `"stop"` (default): the batch is aborted and the returned future
          raises the exception when [`get()`][pykka.Future.get] is called.
          The remaining operations are not executed.
        - `"collect"`: the exception object is placed at the failing
          operation's position in the result list, and the remaining
          operations are still executed.

        In both modes, the side effects of operations already executed are
        *not* rolled back; a batch is not a transaction.

        The batch is consumed by this method: the recorded operations are
        cleared, and the batch proxy can be reused to record a new batch.

        Args:
            on_error: `"stop"` to abort on the first error, or `"collect"`
                to collect errors in the result list and continue.

        Raises:
            ValueError: if `on_error` is invalid or no operations have
                been recorded.

        Returns:
            a future of the list of results, ordered by position

        """
        if on_error not in ("stop", "collect"):
            msg = f"on_error must be 'stop' or 'collect', not {on_error!r}"
            raise ValueError(msg)
        if not self._operations:
            msg = "Cannot execute a batch with no recorded operations"
            raise ValueError(msg)
        operations = tuple(self._operations)
        self._operations.clear()
        message = messages.ProxyBatch(operations=operations, on_error=on_error)
        return self._proxy.actor_ref.ask(message, block=False)


class _BatchCallableProxy(Generic[A]):
    """Records a single method call in a batch."""

    _batch: BatchProxy[A]
    _attr_path: AttrPath

    def __init__(
        self,
        *,
        batch: BatchProxy[A],
        attr_path: AttrPath,
    ) -> None:
        self._batch = batch
        self._attr_path = attr_path

    def __call__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Record a method call in the batch.

        Raises:
            ValueError: if any argument is a [`Future`][pykka.Future], as
                the actor cannot block waiting for another future while
                executing the batch without risking a deadlock.

        """
        for arg in (*args, *kwargs.values()):
            if isinstance(arg, Future):
                msg = (
                    "A Future cannot be used as an argument in a batch "
                    "operation. The batch is executed as a single message "
                    "by the actor, which cannot block waiting for another "
                    "future of the same actor without deadlocking."
                )
                raise ValueError(msg)
        self._batch._operations.append(  # noqa: SLF001
            messages.ProxyCall(
                attr_path=self._attr_path, args=args, kwargs=kwargs
            )
        )


def traversable(obj: T) -> T:
    """Mark an actor attribute as traversable.

    The traversable marker makes the actor attribute's own methods and
    attributes available to users of the actor through an
    [`ActorProxy`][pykka.ActorProxy].

    Used as a function to mark a single attribute:

        class AnActor(pykka.ThreadingActor):
            playback = pykka.traversable(Playback())

        class Playback(object):
            def play(self):
                return True

    This function can also be used as a class decorator, making all instances
    of the class traversable:

        class AnActor(pykka.ThreadingActor):
            playback = Playback()

        @pykka.traversable
        class Playback(object):
            def play(self):
                return True

    The third alternative, and the only way in Pykka < 2.0, is to manually
    mark a class as traversable by setting the `pykka_traversable` attribute
    to `True`:

        class AnActor(pykka.ThreadingActor):
            playback = Playback()

        class Playback(object):
            pykka_traversable = True

            def play(self):
                return True

    When the attribute is marked as traversable, its methods can be executed
    in the context of the actor through an actor proxy::

        proxy = AnActor.start().proxy()
        assert proxy.playback.play().get() is True

    /// note | Version added: Pykka 2.0
    ///
    """
    if hasattr(obj, "__slots__"):
        msg = (
            "pykka.traversable() cannot be used to mark "
            "an object using slots as traversable."
        )
        raise ValueError(msg)
    setattr(obj, "_pykka_traversable", True)  # noqa: B010
    return obj
