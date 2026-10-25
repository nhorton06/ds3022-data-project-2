from prefect import flow

from pipeline.common import REPOS, WINDOW_START
from pipeline.extract import extract
from pipeline.load import load


@flow(name="package-health")
def package_health_flow(repos: list[str] | None = None, window_start: str = WINDOW_START):
    repos = repos or REPOS
    extract(repos, window_start)
    load()


if __name__ == "__main__":
    package_health_flow()
