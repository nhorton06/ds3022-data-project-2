from prefect import flow

from pipeline.common import REPOS, WINDOW_START
from pipeline.extract import extract
from pipeline.load import load
from pipeline.model import model_health
from pipeline.plot import plot
from pipeline.transform import dbt_build
from pipeline.validate import validate


@flow(name="dxx6gu-package-health")
def package_health_flow(repos: list[str] | None = None, window_start: str = WINDOW_START, pr_source: str = "graphql"):
    repos = repos or REPOS
    extract(repos, window_start, pr_source)
    load()
    dbt_build()
    validate()
    model_health()
    plot()


if __name__ == "__main__":
    package_health_flow()
