"""Status vocabularies shared by the API, the workers and the database.

Stored as VARCHAR + CHECK rather than native PostgreSQL ENUM types: adding a new
value to a native enum requires a migration that cannot run inside a
transaction, which makes rolling deploys awkward. VARCHAR + CHECK gives the same
integrity with a far cheaper migration path.
"""

from enum import StrEnum


class BulkActionStatus(StrEnum):
    SCHEDULED = "scheduled"      # accepted, waiting for scheduled_at
    QUEUED = "queued"            # accepted, waiting for a planner worker
    PLANNING = "planning"        # planner is enumerating entities into batches
    PROCESSING = "processing"    # batches are being executed
    COMPLETED = "completed"      # every entity succeeded or was skipped
    COMPLETED_WITH_ERRORS = "completed_with_errors"  # finished, some failures
    FAILED = "failed"            # could not run at all (e.g. planning failed)
    CANCELLED = "cancelled"      # cancelled by the user

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL

    @property
    def is_cancellable(self) -> bool:
        return not self.is_terminal


_TERMINAL = frozenset(
    {
        BulkActionStatus.COMPLETED,
        BulkActionStatus.COMPLETED_WITH_ERRORS,
        BulkActionStatus.FAILED,
        BulkActionStatus.CANCELLED,
    }
)


class BatchStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EntityLogStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class LogReason(StrEnum):
    """Machine-readable reason codes, so a UI can group failures without
    string-matching human-readable messages."""

    UPDATED = "updated"
    DELETED = "deleted"
    DUPLICATE_EMAIL = "duplicate_email"
    DUPLICATE_KEY = "duplicate_key"
    VALIDATION_FAILED = "validation_failed"
    ENTITY_NOT_FOUND = "entity_not_found"
    LEFT_TARGET_SET = "left_target_set"
    NO_CHANGE = "no_change"
    ACTION_CANCELLED = "action_cancelled"
    BATCH_ERROR = "batch_error"
