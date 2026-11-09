"""Permanent message errors independent of broker delivery semantics."""


class PermanentEventError(Exception):
    """The event cannot be accepted without changing its contents."""


class InvalidEventError(PermanentEventError):
    """The incoming event violates the event contract."""


class CampaignConflictError(PermanentEventError):
    """A campaign ID already exists with different business data."""


class CallConflictError(PermanentEventError):
    """A call ID already exists with different business data."""
