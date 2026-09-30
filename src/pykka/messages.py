"""The `pykka.messages` module contains Pykka's own actor messages.

In general, you should not need to use any of these classes. However, they have
been made part of the public API so that certain optimizations can be done
without touching Pykka's internals.

An example is to combine [`ask()`][pykka.ActorRef.ask] and
[`ProxyCall`][pykka.messages.ProxyCall] to call a method on an actor without
having to spend any resources on creating a proxy object:

    reply = actor_ref.ask(
        ProxyCall(
            attr_path=('my_method',),
            args=('foo',),
            kwargs={'bar': 'baz'}
        )
    )

Another example is to use [`tell()`][pykka.ActorRef.tell] instead of
[`ask()`][pykka.ActorRef.ask] for the proxy method call, and thus avoid the
creation of a future for the return value if you don't need it.

It should be noted that these optimizations should only be necessary in very
special circumstances.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, NamedTuple

if TYPE_CHECKING:
    from pykka._types import AttrPath


class _ActorStop(NamedTuple):  # pyright: ignore[reportUnusedClass]
    """Internal message."""


class ProxyCall(NamedTuple):
    """Message to ask the actor to call the method with the arguments.

    /// note | Version added: Pykka 2.0
    ///
    """

    attr_path: AttrPath
    """List with the path from the actor to the method."""

    args: tuple[Any, ...]
    """List with positional arguments."""

    kwargs: dict[str, Any]
    """Dict with keyword arguments."""


class ProxyGetAttr(NamedTuple):
    """Message to ask the actor to return the value of the attribute.

    /// note | Version added: Pykka 2.0
    ///
    """

    attr_path: AttrPath
    """List with the path from the actor to the attribute."""


class ProxySetAttr(NamedTuple):
    """Message to ask the actor to set the attribute to the value.

    /// note | Version added: Pykka 2.0
    ///
    """

    attr_path: AttrPath
    """List with the path from the actor to the attribute."""

    value: Any
    """The value to set the attribute to."""


#: The proxy operations that can be part of a batch.
ProxyBatchOperation = ProxyCall | ProxyGetAttr | ProxySetAttr


class ProxyBatch(NamedTuple):
    """Message to ask the actor to execute a batch of proxy operations.

    The operations are executed in order as a single inbox message, so no
    other messages are processed by the actor in between the operations.

    The reply is a list with one result per operation, in the same order as
    the operations.

    If `on_error` is `"stop"`, the first failing operation aborts the batch
    and its exception is set on the reply future. Operations already executed
    are *not* rolled back; their side effects remain.

    If `on_error` is `"continue"`, all operations are executed, and the
    result list contains the exception instance at the position of each
    failed operation.
    """

    operations: tuple[ProxyBatchOperation, ...]
    """The proxy operations to execute, in order."""

    on_error: Literal["stop", "continue"] = "stop"
    """Whether to abort on the first error or collect errors and continue."""
