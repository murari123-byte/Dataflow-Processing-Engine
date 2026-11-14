import json
import os
import uuid

from datetime import datetime, timezone
from time import perf_counter


from src.config import RUN_LOG_PATH


def current_timestamp():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def start_run():
    """Create a new pipeline execution record."""
    return {
        "run_id": str(uuid.uuid4()),
        "started_at": current_timestamp(),
        "_start_time": perf_counter(),
    }


def finish_run(
    run,
    status,
    input_records=0,
    valid_records=0,
    rejected_records=0,
    aggregation_groups=0,
    error=None,
):
    """Complete and persist a pipeline execution record."""

    finished_at = current_timestamp()

    duration = round(
        perf_counter() - run["_start_time"],
        3,
    )

    record = {
        "run_id": run["run_id"],
        "started_at": run["started_at"],
        "finished_at": finished_at,
        "status": status,
        "duration_seconds": duration,
        "input_records": input_records,
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "aggregation_groups": aggregation_groups,
        "error": str(error) if error else None,
    }

    os.makedirs(os.path.dirname(RUN_LOG_PATH), exist_ok=True)

    with open(RUN_LOG_PATH, "a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")

    return record
