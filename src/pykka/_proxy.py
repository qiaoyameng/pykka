from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload

from pykka import ActorDeadError, messages
from pykka._introspection import AttrInfo, introspect_attrs

if TYPE_CHECKING:
    from pykka import Actor, ActorRef, Future
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

    def batch(
        self,
        *,
        on_error: Literal["stop", "continue"] = "stop",
    ) -> BatchProxy[A]:
        """Create a batch of operations to execute as a single actor message.

        Reading several attributes and calling several methods through a
        regular proxy sends one inbox message per operation, so other
        messages may be processed by the actor in between them. A batch
        records the operations and sends them as a single inbox message,
        so no other messages are processed by the actor in between them.

        Example:
            ```py
            proxy = AnActor.start().proxy()

            batch = proxy.batch()
            batch.some_attr            # records an attribute read
            batch.a_method(1, x=2)     # records a method call
            batch.other_attr = "foo"   # records an attribute assignment

            results = batch.execute().get()
            ```

        [`execute()`][pykka.BatchProxy.execute] returns a future for a list
        with one result per recorded operation, in the order the operations
        were recorded.

        Args:
            on_error: `"stop"` aborts the batch at the first failing
                operation and reraises its exception when the result is
                retrieved. `"continue"` executes all operations and places
                the exception instance at the failing operation's position
                in the result list.

        Returns:
            a [`BatchProxy`][pykka.BatchProxy] recording operations for this
            proxy's actor.

        """
        return BatchProxy(proxy=self, on_error=on_error)


class BatchProxy(Generic[A]):
    """Records proxy operations and executes them as a single actor message.

    Created by [`ActorProxy.batch()`][pykka.ActorProxy.batch]. See its
    documentation for an example.

    Attribute reads, method calls, and attribute assignments are recorded
    instead of sent. Attribute paths follow the same rules as for
    [`ActorProxy`][pykka.ActorProxy]: only attributes marked with
    [`pykka.traversable()`][pykka.traversable] can be traversed.

    The recorded operations are sent to the actor as a single inbox message
    by [`execute()`][pykka.BatchProxy.execute]. While the actor executes the
    batch, it cannot process any other messages. This also means that code
    run by the batch must not block waiting for a future replied to by the
    same actor, as that would deadlock the actor. Pykka detects this and
    raises [`RuntimeError`][RuntimeError] instead of deadlocking.

    Batches are *not* transactions. If an operation fails, the side effects
    of the operations already executed are not rolled back.
    """

    _proxy: ActorProxy[A]
    _attr_path: AttrPath
    _operations: list[messages.ProxyBatchOperation]
    _on_error: Literal["stop", "continue"]

    def __init__(
        self,
        *,
        proxy: ActorProxy[A],
        attr_path: AttrPath | None = None,
        operations: list[messages.ProxyBatchOperation] | None = None,
        on_error: Literal["stop", "continue"] = "stop",
    ) -> None:
        if on_error not in ("stop", "continue"):
            msg = f"on_error must be 'stop' or 'continue', not {on_error!r}"
            raise ValueError(msg)
        self._proxy = proxy
        self._attr_path = attr_path or proxy._attr_path  # noqa: SLF001
        self._operations = operations if operations is not None else []
        self._on_error = on_error

    def __repr__(self) -> str:
        return (
            f"<BatchProxy for {self._proxy.actor_ref}, "
            f"attr_path={self._attr_path!r}, "
            f"operations={len(self._operations)}>"
        )

    def __getattr__(self, name: str) -> Any:
        """Record a field read, or get a callable or traversable recorder."""
        proxy = self._proxy
        attr_path: AttrPath = (*self._attr_path, name)

        known_attrs = proxy._known_attrs  # noqa: SLF001
        if attr_path not in known_attrs:
            proxy._known_attrs = known_attrs = introspect_attrs(  # noqa: SLF001
                root=proxy._actor,  # noqa: SLF001
                proxy=proxy,
            )

        attr_info = known_attrs.get(attr_path)
        if attr_info is None:
            msg = f"{self} has no attribute {name!r}"
            raise AttributeError(msg)

        if attr_info.callable:
            return _BatchCallableProxy(batch=self, attr_path=attr_path)

        if attr_info.traversable:
            return BatchProxy(
                proxy=proxy,
                attr_path=attr_path,
                operations=self._operations,
                on_error=self._on_error,
            )

        self._operations.append(messages.ProxyGetAttr(attr_path=attr_path))
        return None

    def __setattr__(
        self,
        name: str,
        value: Any,
    ) -> None:
        """Record a field assignment."""
        if name.startswith("_"):
            return super().__setattr__(name, value)
        attr_path = (*self._attr_path, name)
        self._operations.append(messages.ProxySetAttr(attr_path=attr_path, value=value))
        return None

    @overload
    def execute(
        self,
        *,
        block: Literal[False],
        timeout: float | None = None,
    ) -> Future[list[Any]]: ...

    @overload
    def execute(
        self,
        *,
        block: Literal[True],
        timeout: float | None = None,
    ) -> list[Any]: ...

    @overload
    def execute(
        self,
        *,
        block: bool = False,
        timeout: float | None = None,
    ) -> Future[list[Any]] | list[Any]: ...

    def execute(
        self,
        *,
        block: bool = False,
        timeout: float | None = None,
    ) -> Future[list[Any]] | list[Any]:
        """Send the recorded operations to the actor as a single message.

        The actor executes the operations in order, without processing any
        other messages in between.

        With `on_error="stop"`, the first failing operation aborts the
        batch, and its exception is reraised by
        [`get()`][pykka.Future.get] on the returned future. The remaining
        operations are not executed. Side effects of operations already
        executed are *not* rolled back.

        With `on_error="continue"`, all operations are executed, and the
        result list contains the exception instance at the position of each
        failed operation.

        Args:
            block: whether to block while waiting for the results
            timeout: seconds to wait before timeout if blocking

        Raises:
            ValueError: if the batch has no recorded operations
            Timeout: if timeout is reached if blocking
            Exception: any exception from the first failing operation, if
                blocking and `on_error` is `"stop"`

        Returns:
            a future for the list of results if not blocking, or the list
            of results if blocking, with one result per recorded operation,
            in the order the operations were recorded

        """
        if not self._operations:
            msg = "Cannot execute a batch with no operations"
            raise ValueError(msg)
        message = messages.ProxyBatch(
            operations=tuple(self._operations),
            on_error=self._on_error,
        )
        return self._proxy.actor_ref.ask(message, block=block, timeout=timeout)


class _BatchCallableProxy(Generic[A]):
    """Records a method call in a batch instead of sending it."""

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
        """Record the method call in the batch."""
        self._batch._operations.append(  # noqa: SLF001
            messages.ProxyCall(attr_path=self._attr_path, args=args, kwargs=kwargs)
        )
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
