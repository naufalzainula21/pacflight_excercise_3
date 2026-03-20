#!/bin/bash
# run_pipeline.sh — triggered by cron daily at midnight
# crontab entry: 0 0 * * * /path/to/run_pipeline.sh >> /path/to/logs/cron.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"

echo "[$(date)] Starting PacTravel pipeline..." | tee -a "$LOG_FILE"

# Activate virtual environment if present
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# Run Luigi master pipeline
python -m luigi --module src.pipeline MasterPipeline --local-scheduler 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date)] Pipeline completed successfully." | tee -a "$LOG_FILE"
else
    echo "[$(date)] Pipeline FAILED with exit code $EXIT_CODE." | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
