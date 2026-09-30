"""Bounded actor mailbox.

The mailbox is the FIFO queue of envelopes between an actor and the outside
world. In addition to the message queue, it has an independent stop channel,
so that stopping an actor can never be blocked by a full mailbox.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import TYPE_CHECKING, Any

from pykka._exceptions import ActorDeadError, MailboxFull

if TYPE_CHECKING:
    from pykka._envelope import Envelope
    from pykka._future import Future

__all__ = ["Mailbox"]


class Mailbox:
    """A FIFO mailbox with optional bounded capacity.

    Args:
        maxsize: maximum number of messages the mailbox can hold. If `None`
            or zero or less, the mailbox is unbounded.

    The mailbox has two independent channels:

    - The message queue, which is bounded by `maxsize`. Producers use
      [`put()`][pykka.Mailbox.put] and the actor uses
      [`get()`][pykka.Mailbox.get].
    - A stop channel, via
      [`request_stop()`][pykka.Mailbox.request_stop],
      [`terminate()`][pykka.Mailbox.terminate], and
      [`finish_graceful_stop()`][pykka.Mailbox.finish_graceful_stop]. It
      always makes progress, even when the message queue is full, so an
      actor can always be stopped.

    Every message that is successfully put in the mailbox is guaranteed to
    either be delivered to the actor or get an explicit error reply; a put
    either succeeds atomically or it raises and no message was enqueued.
    """

    def __init__(self, maxsize: int | None = None) -> None:
        self._maxsize: int | None = (
            maxsize if (maxsize is not None and maxsize > 0) else None
        )
        self._queue: deque[Envelope[Any]] = deque()
        self._condition = threading.Condition()
        self._stopping = False
        self._closed = False
        self._stop_acks: list[Future[Any]] = []
        self._in_flight = 0
        self.consumer_ident: int | None = None
        """Thread identifier of the mailbox's sole consumer (the actor)."""

    def qsize(self) -> int:
        """Return the number of messages currently in the mailbox."""
        with self._condition:
            return len(self._queue) + self._in_flight

    def empty(self) -> bool:
        """Return `True` if the mailbox currently contains no messages."""
        with self._condition:
            return not self._queue and self._in_flight == 0

    def full(self) -> bool:
        """Return `True` if the mailbox is bounded and currently full."""
        with self._condition:
            return (
                self._maxsize is not None
                and len(self._queue) + self._in_flight >= self._maxsize
            )

    def _load(self) -> int:
        return len(self._queue) + self._in_flight

    def put(
        self,
        envelope: Envelope[Any],
        /,
        timeout: float | None = None,
    ) -> None:
        """Put an envelope in the mailbox.

        If the mailbox is unbounded, or it has free capacity, the envelope is
        enqueued immediately.

        If the mailbox is full:

        - with `timeout` set to `0`, [`MailboxFull`][pykka.MailboxFull] is
          raised immediately,
        - with a positive `timeout`, the call waits up to `timeout` seconds
          for free capacity and raises [`MailboxFull`][pykka.MailboxFull] if
          the timeout expires,
        - with `timeout` set to `None` (the default), it waits until free
          capacity is available.

        Args:
            envelope: the envelope to enqueue
            timeout: seconds to wait for free capacity, or `0` for
                non-blocking, or `None` to wait forever

        Raises:
            MailboxFull: if the mailbox was full and no capacity became
                available in time
            ActorDeadError: if the actor was stopping or stopped while
                waiting
        """
        with self._condition:
            if self._maxsize is not None:
                if timeout is None:
                    while self._load() >= self._maxsize:
                        if self._stopping or self._closed:
                            raise ActorDeadError(
                                "Actor stopped before the message could be enqueued"
                            )
                        self._condition.wait()
                elif timeout <= 0:
                    if (
                        self._load() >= self._maxsize
                        and not self._stopping
                        and not self._closed
                    ):
                        raise MailboxFull("Actor mailbox is full")
                else:
                    end = time.monotonic() + timeout
                    while self._load() >= self._maxsize:
                        if self._stopping or self._closed:
                            raise ActorDeadError(
                                "Actor stopped before the message could be enqueued"
                            )
                        remaining = end - time.monotonic()
                        if remaining <= 0:
                            raise MailboxFull(
                                "Actor mailbox is full and enqueue timed out"
                            )
                        self._condition.wait(remaining)

            if self._stopping or self._closed:
                raise ActorDeadError(
                    "Actor stopped before the message could be enqueued"
                )

            self._queue.append(envelope)
            self._condition.notify_all()

    def get(self, *, release_previous: bool = True) -> Envelope[Any] | None:
        """Get the next envelope for the actor, blocking until one is ready.

        Returns `None` when a stop has been requested and all messages that
        were already in the mailbox at that point have been delivered. While
        the actor is stopping but messages remain, those messages are still
        delivered so that graceful shutdown processes everything that was
        successfully enqueued.

        Args:
            release_previous: when `True` (the default), the capacity slot
                held by the previously delivered, now processed envelope is
                released atomically with taking the next one, so producers
                can never observe a temporarily overbooked mailbox.
        """
        with self._condition:
            if release_previous and self._in_flight > 0:
                self._in_flight -= 1
            while not self._queue and not self._stopping and not self._closed:
                self._condition.wait()
            if not self._queue:
                self._condition.notify_all()
                return None
            envelope = self._queue.popleft()
            self._in_flight += 1
            # Do not wake producers here: popping does not release capacity,
            # the in-flight message still holds its slot. Producers are only
            # woken once a slot is free.
            return envelope

    def request_stop(self, ack: Future[Any] | None = None) -> None:
        """Ask the actor to stop after processing already-enqueued messages.

        This is the independent stop channel: it always returns immediately,
        regardless of how full the message queue is. Any producer currently
        waiting in [`put()`][pykka.Mailbox.put] is woken up and receives an
        [`ActorDeadError`][pykka.ActorDeadError].

        Args:
            ack: optional future that is completed when the actor has
                finished stopping
        """
        fail_ack = False
        with self._condition:
            if self._closed:
                fail_ack = True
            else:
                self._stopping = True
                if ack is not None:
                    self._stop_acks.append(ack)
                self._condition.notify_all()
        if fail_ack and ack is not None:
            ack.set_exception(
                exc_info=(
                    ActorDeadError,
                    ActorDeadError("Actor already stopped"),
                    None,
                )
            )

    def finish_graceful_stop(self) -> None:
        """Close the mailbox after the actor has drained its queue.

        Wakes every producer waiting in [`put()`][pykka.Mailbox.put] and
        completes the acknowledgement futures registered with
        [`request_stop()`][pykka.Mailbox.request_stop].
        """
        with self._condition:
            if self._closed:
                return
            self._stopping = True
            self._closed = True
            acks = self._stop_acks
            self._stop_acks = []
            self._condition.notify_all()
        for ack in acks:
            ack.set(None)

    def terminate(self) -> None:
        """Close the mailbox immediately because the actor is dying.

        Unlike a graceful stop, pending stop acknowledgements are failed with
        [`ActorDeadError`][pykka.ActorDeadError]. Messages already enqueued
        are left in the queue so the actor's teardown can error-reply them.
        """
        with self._condition:
            if self._closed:
                self._stopping = True
                self._condition.notify_all()
                return
            self._stopping = True
            self._closed = True
            acks = self._stop_acks
            self._stop_acks = []
            self._condition.notify_all()
        for ack in acks:
            ack.set_exception(
                exc_info=(
                    ActorDeadError,
                    ActorDeadError("Actor stopped before handling the stop request"),
                    None,
                )
            )
