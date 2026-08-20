import enum


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    CANCELING = "canceling"
    CANCELED = "canceled"
    COMPLETED = "completed"
    FAILED = "failed"


class UrlStatus(str, enum.Enum):
    DISCOVERED = "discovered"
    QUEUED = "queued"
    CLAIMED = "claimed"
    FETCHING = "fetching"
    PROCESSING = "processing"
    RETRY_WAIT = "retry_wait"
    DONE = "done"
    FAILED_PERMANENT = "failed_permanent"
    CANCELED = "canceled"
    SKIPPED_CHILD_SPAWNED = "skipped_child_spawned"
