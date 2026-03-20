"""Luigi event-based alerting — logs failures to alerts.log."""

import logging
import os
from datetime import datetime

import luigi

ALERTS_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "alerts.log")

logger = logging.getLogger(__name__)


def _write_alert(message: str) -> None:
    os.makedirs(os.path.dirname(ALERTS_LOG), exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(ALERTS_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    logger.error("ALERT: %s", message)


@luigi.Task.event_handler(luigi.Event.FAILURE)
def on_task_failure(task, exception):
    message = (
        f"TASK FAILURE | task={task.__class__.__name__} "
        f"params={task.to_str_params()} | error={exception}"
    )
    _write_alert(message)


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def on_task_success(task):
    logger.info("Task succeeded: %s", task.__class__.__name__)
