from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from pykka import Future


T = TypeVar("T")


class Envelope(Generic[T]):
    """Envelope to add metadata to a message.

    This is an internal type and is not part of the public API.
    """

    # Using slots speeds up envelope creation with ~20%
    __slots__ = ["message", "reply_to", "context"]

    message: T
    """The message to send."""

    reply_to: Future[Any] | None
    """The future to reply to if there is a response."""

    context: contextvars.Context | None
    """Selected context variables from the sender, if propagation is enabled."""

    def __init__(
        self,
        message: T,
        reply_to: Future[Any] | None = None,
        context: contextvars.Context | None = None,
    ) -> None:
        self.message = message
        self.reply_to = reply_to
        self.context = context

    def __repr__(self) -> str:
        return (
            f"Envelope(message={self.message!r}, reply_to={self.reply_to!r}, "
            f"context={self.context!r})"
        )
