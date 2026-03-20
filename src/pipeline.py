"""Master Luigi pipeline — EL tasks followed by dbt run & snapshot."""

import logging
import os
import subprocess

import luigi

# Register event handlers before anything runs
import src.alert  # noqa: F401
from src.extract_load.tasks import ALL_EL_TASKS

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_pactravel")
DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_pactravel")


class AllExtractLoad(luigi.WrapperTask):
    """Runs all 7 EL tasks (Luigi runs them in parallel where possible)."""

    def requires(self):
        return [TaskClass() for TaskClass in ALL_EL_TASKS]


class DbtSnapshot(luigi.Task):
    """Runs `dbt snapshot` to materialise SCD Type 2 dim_customer."""

    def requires(self):
        return AllExtractLoad()

    def output(self):
        return luigi.LocalTarget("temp/dbt_snapshot.done")

    def run(self):
        _run_dbt_command(["dbt", "snapshot"])
        with self.output().open("w") as f:
            f.write("done")


class DbtRun(luigi.Task):
    """Runs `dbt run` to build all dimension and fact models."""

    def requires(self):
        return DbtSnapshot()

    def output(self):
        return luigi.LocalTarget("temp/dbt_run.done")

    def run(self):
        _run_dbt_command(["dbt", "run"])
        with self.output().open("w") as f:
            f.write("done")


class MasterPipeline(luigi.WrapperTask):
    """Entry point: EL → dbt snapshot → dbt run."""

    def requires(self):
        return DbtRun()


def _run_dbt_command(cmd: list) -> None:
    full_cmd = cmd + [
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]
    logger.info("Running: %s", " ".join(full_cmd))
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt command failed (exit {result.returncode}):\n{result.stderr}"
        )


if __name__ == "__main__":
    import luigi
    luigi.build([MasterPipeline()], local_scheduler=True)
