"""Register the monthly Prefect deployment (runs on the SDS Prefect server's Kubernetes work pool).

Run once with `python deploy.py` after creating the Secret block (see DEPLOYMENTS.md).
"""

from pathlib import Path

from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import package_health_flow

GIT_URL = "https://github.com/nhorton06/ds3022-data-project-2.git"
WORK_POOL = "uvasds-k8s-pool"
SECRET_BLOCK = "dxx6gu-pat"  # Prefect Secret block holding a GitHub token with the `repo` scope

# The worker's image doesn't have dbt etc., so install the pinned requirements when each run starts
PACKAGES = " ".join(Path(__file__).with_name("requirements.txt").read_text().split())

if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load(SECRET_BLOCK)},  # the block itself, so the token isn't stored
        ),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name="dxx6gu-deployment",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
            "env": {"EXTRA_PIP_PACKAGES": PACKAGES},
        },
    )
