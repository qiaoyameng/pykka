from __future__ import annotations

import logging
import queue
import sys
import threading
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar

from pykka._exceptions import ActorDeadError, CancelledError

if TYPE_CHECKING:
    from pykka._future import Future
    from pykka._types import OptExcInfo

__all__ = ["DeferredReply", "DeferredReplyHandle"]


logger = logging.getLogger("pykka")


T = TypeVar("T")


class _State(Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TERMINATED = "terminated"


class DeferredReplyHandle(Generic[T]):
    """A one-shot handle for replying to a message later.

    Return this from [`Actor.on_receive()`][pykka.Actor.on_receive] instead of
    returning a value immediately. The actor can then process other messages
    while the sender waits for the reply.

    Args:
        on_cancel: optional callback called when the reply is cancelled or when
            its actor stops before the reply completes.
    """

    __slots__ = (
        "_cancel_callback",
        "_condition",
        "_exc_info",
        "_future",
        "_on_complete",
        "_state",
        "_value",
    )

    def __init__(
        self,
        *,
        on_cancel: Callable[[], None] | None = None,
    ) -> None:
        self._cancel_callback = on_cancel
        self._condition = threading.Condition()
        self._exc_info: OptExcInfo | None = None
        self._future: Future[T] | None = None
        self._on_complete: Callable[[], None] | None = None
        self._state = _State.PENDING
        self._value: T | None = None

    def __repr__(self) -> str:
        with self._condition:
            return f"<pykka.DeferredReplyHandle {self._state.value}>"

    def set(
        self,
        value: T | None = None,
    ) -> None:
        """Complete the reply successfully.

        Args:
            value: the value returned to the sender

        Raises:
            queue.Full: if the reply has already been completed
        """
        with self._condition:
            future, on_complete = self._complete(_State.SUCCEEDED)
            self._value = value

        if future is not None:
            future.set(value)
        if on_complete is not None:
            on_complete()

    def set_exception(
        self,
        exc_info: OptExcInfo | BaseException | None = None,
    ) -> None:
        """Complete the reply with an exception.

        An exception instance or an `exc_info` three-tuple can be provided. If
        neither is provided, the current exception from
        [`sys.exc_info()`][sys.exc_info] is used.

        Args:
            exc_info: the exception returned to the sender

        Raises:
            queue.Full: if the reply has already been completed
        """
        normalized_exc_info: OptExcInfo
        if isinstance(exc_info, BaseException):
            normalized_exc_info = (
                type(exc_info),
                exc_info,
                exc_info.__traceback__,
            )
        elif exc_info is not None:
            normalized_exc_info = exc_info
        else:
            normalized_exc_info = sys.exc_info()

        assert len(normalized_exc_info) == 3

        with self._condition:
            future, on_complete = self._complete(_State.FAILED)
            self._exc_info = normalized_exc_info

        if future is not None:
            future.set_exception(normalized_exc_info)
        if on_complete is not None:
            on_complete()

    def cancel(self) -> bool:
        """Cancel the reply.

        The sender receives [`CancelledError`][pykka.CancelledError]. If an
        `on_cancel` callback was provided, it is called after the sender's
        future is cancelled.

        Returns:
            `True` if this call cancelled the reply, otherwise `False`
        """
        with self._condition:
            try:
                future, on_complete = self._complete(_State.CANCELLED)
            except queue.Full:
                return False
            exc_info: OptExcInfo = (
                CancelledError,
                CancelledError("Deferred reply was cancelled"),
                None,
            )
            self._exc_info = exc_info
            cancel_callback = self._cancel_callback

        if future is not None:
            future.set_exception(exc_info)
        if on_complete is not None:
            on_complete()
        if cancel_callback is not None:
            cancel_callback()
        return True

    def _bind(
        self,
        future: Future[T],
        register: Callable[[DeferredReplyHandle[T]], None],
        unregister: Callable[[DeferredReplyHandle[T]], None],
    ) -> bool:
        """Bind the handle to the sender's future.

        Returns `False` if the handle was already completed, in which case the
        result is set on `future` by this method.
        """
        with self._condition:
            if self._future is not None:
                msg = "Deferred reply handle is already bound to a request"
                raise RuntimeError(msg)

            if self._state is not _State.PENDING:
                if self._state is _State.SUCCEEDED:
                    future.set(self._value)
                else:
                    assert self._exc_info is not None
                    future.set_exception(self._exc_info)
                return False

            self._future = future

            def complete() -> None:
                unregister(self)

            self._on_complete = complete
            register(self)
            return True

    def _terminate(
        self,
        actor_name: str,
    ) -> bool:
        """Terminate a pending reply because its actor is stopping."""
        with self._condition:
            try:
                future, on_complete = self._complete(_State.TERMINATED)
            except queue.Full:
                return False
            exception = ActorDeadError(
                f"{actor_name} stopped before completing the deferred reply"
            )
            exc_info: OptExcInfo = (ActorDeadError, exception, None)
            self._exc_info = exc_info
            cancel_callback = self._cancel_callback

        if future is not None:
            future.set_exception(exc_info)
        if on_complete is not None:
            on_complete()
        if cancel_callback is not None:
            try:
                cancel_callback()
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Exception raised by deferred reply cancellation callback "
                    f"while {actor_name} was stopping"
                )
        return True

    def _complete(
        self,
        state: _State,
    ) -> tuple[Future[T] | None, Callable[[], None] | None]:
        if self._state is not _State.PENDING:
            raise queue.Full
        self._state = state
        return self._future, self._on_complete


DeferredReply = DeferredReplyHandle
