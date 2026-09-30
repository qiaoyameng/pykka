from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from pykka import Future
    from pykka._types import OptExcInfo

__all__ = ["DeferredReply"]


T = TypeVar("T")


class DeferredReply(Generic[T]):
    """A handle for completing an actor reply at a later time.

    Return an instance from [`Actor.on_receive()`][pykka.Actor.on_receive] to
    acknowledge a message without blocking the actor's inbox. The caller's
    [`Future`][pykka.Future] is completed later by calling
    [`set()`][pykka.DeferredReply.set] or
    [`set_exception()`][pykka.DeferredReply.set_exception].

    A timeout experienced by the caller does not cancel the ongoing operation.
    To support explicit cancellation, pass `on_cancel`; it is called if and
    only if the caller successfully cancels the future before it is completed.
    The callback is normally used to send another message back to the actor.
    """

    _future: Future[T] | None
    _bound: bool

    def __init__(
        self,
        *,
        on_cancel: Callable[[], Any] | None = None,
    ) -> None:
        super().__init__()
        self._future = None
        self._bound = False
        self._pending_replies: set[DeferredReply[Any]] | None = None
        self._pending_replies_lock: threading.Lock | None = None
        self._on_cancel: Callable[[], Any] | None = on_cancel

    def __repr__(self) -> str:
        return "<pykka.DeferredReply>"

    def set(
        self,
        value: T | None = None,
    ) -> None:
        """Complete the reply successfully.

        Args:
            value: the value returned to the caller

        Raises:
            queue.Full: if the reply has already been completed or cancelled
            RuntimeError: if the handle was not returned by `on_receive()`
        """
        self._unregister_pending_reply()
        self._get_future().set(value)

    def set_exception(
        self,
        exc_info: OptExcInfo | None = None,
    ) -> None:
        """Complete the reply with an exception.

        See [`Future.set_exception()`][pykka.Future.set_exception] for the
        accepted `exc_info` formats.

        Raises:
            queue.Full: if the reply has already been completed or cancelled
            RuntimeError: if the handle was not returned by `on_receive()`
        """
        self._unregister_pending_reply()
        self._get_future().set_exception(exc_info)

    def _bind(
        self,
        future: Future[T] | None,
        pending_replies: set[DeferredReply[Any]],
        pending_replies_lock: threading.Lock,
    ) -> None:
        if self._bound:
            msg = "DeferredReply has already been returned from on_receive()"
            raise RuntimeError(msg)
        self._bound = True

        if future is None:
            msg = "DeferredReply can only be returned in response to ask()"
            raise RuntimeError(msg)

        self._future = future
        self._pending_replies = pending_replies
        self._pending_replies_lock = pending_replies_lock

        def canceled() -> None:
            self._unregister_pending_reply()
            if self._on_cancel is not None:
                self._on_cancel()

        already_cancelled = False
        try:
            already_cancelled = future._set_cancel_hook(canceled)  # noqa: SLF001
        except NotImplementedError:
            pass

        with pending_replies_lock:
            pending_replies.add(self)

        if already_cancelled:
            canceled()

    def _unregister_pending_reply(self) -> None:
        if self._pending_replies is None or self._pending_replies_lock is None:
            return

        with self._pending_replies_lock:
            self._pending_replies.discard(self)

    def _fail(
        self,
        exc_info: OptExcInfo,
    ) -> None:
        future = self._get_future()
        try:
            future.set_exception(exc_info)
        except queue.Full:
            pass

    def _get_future(self) -> Future[T]:
        future = self._future
        if future is None:
            msg = "DeferredReply must be returned from on_receive() first"
            raise RuntimeError(msg)
        return future
