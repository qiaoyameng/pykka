__all__ = ["ActorDeadError", "MailboxFull", "Timeout"]


class ActorDeadError(Exception):
    """Exception raised when trying to use a dead or unavailable actor."""


class MailboxFull(Exception):
    """Exception raised when a bounded mailbox cannot accept a message.

    Raised by [`ActorRef.tell()`][pykka.ActorRef.tell] and
    [`ActorRef.ask()`][pykka.ActorRef.ask] when the actor's mailbox has a
    finite capacity, it is currently full, and the message could not be
    enqueued:

    - immediately, if no enqueue timeout was given, or
    - before the given enqueue timeout expired.

    It is also set on futures returned from non-blocking calls in the same
    situation.
    """


class Timeout(Exception):  # noqa: N818
    """Exception raised at future timeout."""
