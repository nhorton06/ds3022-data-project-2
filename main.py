from prefect import flow

from pipeline.common import REPOS, WINDOW_START
from pipeline.extract import extract
from pipeline.load import load
from pipeline.transform import dbt_build


@flow(name="package-health")
def package_health_flow(repos: list[str] | None = None, window_start: str = WINDOW_START):
    repos = repos or REPOS
    extract(repos, window_start)
    load()
    dbt_build()


if __name__ == "__main__":
    package_health_flow()
