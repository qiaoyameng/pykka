__all__ = ["ActorDeadError", "CancelledError", "Timeout"]


class ActorDeadError(Exception):
    """Exception raised when trying to use a dead or unavailable actor."""


class CancelledError(Exception):
    """Exception raised when a deferred reply is cancelled."""


class Timeout(Exception):  # noqa: N818
    """Exception raised at future timeout."""
