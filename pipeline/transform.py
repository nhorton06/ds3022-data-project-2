"""Task 3: run `dbt build` (staging, fact, and mart models plus all tests). The only place dbt runs."""

from prefect import task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipeline.common import PROJECT_DIR, TaskLogger


@task
def dbt_build():
    """Task 3: build every dbt model and run every test. Raises, failing the flow, if any model or test fails.

    dbt writes its own log to logs/dbt.log; this task's summary goes to logs/transform.log.
    """
    log = TaskLogger("transform")
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=str(PROJECT_DIR), profiles_dir=str(PROJECT_DIR))
    )
    try:
        result = runner.invoke(["build"])
    except ValueError as exc:
        log.error(f"dbt build failed: {exc}")
        raise

    nodes = result.result.results
    for node in nodes:
        log.info(f"{str(node.status).upper():7} {node.node.resource_type:5} {node.node.name}")
    log.info(f"dbt build passed: {len(nodes)} models and tests")
    return len(nodes)
