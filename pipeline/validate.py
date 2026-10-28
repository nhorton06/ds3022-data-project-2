"""Task 4: validate the pipeline end to end in Python (no dbt) and describe the data before modeling."""

import json

import duckdb
from prefect import task

from pipeline.common import DB_PATH, RAW_DIR, TaskLogger
from pipeline.load import TABLES

MERGE_OUTLIER_HOURS = 365 * 24  # merged more than a year after it was opened
WEEKLY_IQR_MULTIPLIER = 3  # Tukey's "far out" fence: Q3 + 3 * IQR

# endpoint folder -> (staging model, natural key field in each landed record, rows staging drops on purpose)
STAGING = {
    "commits": ("stg_commits", "sha", "false"),
    "pulls": ("stg_pull_requests", "number", "false"),
    "pr_reviews": ("stg_pr_reviews", "id", "(payload->>'state') = 'PENDING'"),
    "releases": ("stg_releases", "id", "(payload->>'draft')::BOOLEAN"),
}


def check_landed_records(con, log, endpoint):
    """Every distinct (repo, id) in the landed files must be in the raw table. Returns failure messages."""
    table = TABLES[endpoint][0]
    key = STAGING[endpoint][1]
    landed = set()
    for path in RAW_DIR.glob(f"*/{endpoint}/*.json"):
        page = json.loads(path.read_text())
        landed.update((page["repo"], str(record[key])) for record in page["data"])
    loaded = set(con.execute(f"SELECT repo, id::VARCHAR FROM {table}").fetchall())

    failures = []
    for repo in sorted({repo for repo, _ in landed}):
        repo_landed = {k for k in landed if k[0] == repo}
        missing = repo_landed - loaded
        log.info(f"{repo} {endpoint}: {len(repo_landed)} landed records, {len(missing)} missing from {table}")
        if missing:
            failures.append(f"{repo} {endpoint}: {len(missing)} landed records missing from {table}")
    return failures


def check_staging_counts(con, log, endpoint):
    """Raw rows (minus rows staging drops on purpose) must equal staging rows, per repo. Returns failure messages."""
    table = TABLES[endpoint][0]
    model, _, dropped = STAGING[endpoint]
    rows = con.execute(f"""
        SELECT r.repo, r.raw_rows, r.dropped_rows, coalesce(s.staging_rows, 0)
        FROM (
            SELECT repo, count(*) AS raw_rows, count(*) FILTER (WHERE {dropped}) AS dropped_rows
            FROM {table} GROUP BY repo
        ) r
        LEFT JOIN (SELECT repo, count(*) AS staging_rows FROM {model} GROUP BY repo) s USING (repo)
        ORDER BY r.repo
    """).fetchall()

    failures = []
    for repo, raw_rows, dropped_rows, staging_rows in rows:
        log.info(f"{repo} {endpoint}: {raw_rows} raw rows, {dropped_rows} dropped on purpose, {staging_rows} in {model}")
        if raw_rows - dropped_rows != staging_rows:
            failures.append(f"{repo} {endpoint}: {table} has {raw_rows - dropped_rows} rows but {model} has {staging_rows}")
    return failures


def check_monthly_commits(con, log):
    """Every repo must have commits in every complete month (the current, partial month is exempt)."""
    empty = con.execute("""
        SELECT repo, month FROM mart_repo_monthly
        WHERE NOT is_partial_month AND commits = 0
        ORDER BY repo, month
    """).fetchall()
    months = con.execute("SELECT count(DISTINCT month) FROM mart_repo_monthly WHERE NOT is_partial_month").fetchone()[0]
    log.info(f"Commits in every complete month: checked {months} months per repo, {len(empty)} empty")
    return [f"{repo}: no commits in {month}" for repo, month in empty]


def flag_merge_outliers(con, log):
    """List human PRs merged more than a year after they were opened. Kept, not dropped. Returns the flagged rows."""
    flagged = con.execute("""
        SELECT repo, number, round(hours_to_merge / 24, 1) AS days_to_merge,
               created_at::DATE AS created, merged_at::DATE AS merged
        FROM fct_pull_requests
        WHERE NOT is_bot AND is_merged AND hours_to_merge > ?
        ORDER BY repo, hours_to_merge DESC
    """, [MERGE_OUTLIER_HOURS]).df()
    log.info(
        f"Time-to-merge outliers (merged > {MERGE_OUTLIER_HOURS} h = 1 year after opening): {len(flagged)} PRs, "
        f"kept\n{flagged.to_string(index=False)}"
    )
    return flagged


def flag_commit_outliers(con, log):
    """List weeks with human commits above Q3 + 3 * IQR for that repo. Kept, not dropped. Returns the flagged rows."""
    flagged = con.execute(f"""
        WITH run AS (
            SELECT date_trunc('week', max(fetched_at)) AS run_week FROM raw_commits
        ),
        -- every complete week from the window start (a Monday) to the week before the run week
        weeks AS (
            SELECT generate_series::DATE AS week
            FROM generate_series(TIMESTAMP '2024-01-01', (SELECT run_week FROM run) - INTERVAL 7 DAY, INTERVAL 7 DAY)
        ),
        counts AS (
            SELECT repo, date_trunc('week', committed_at)::DATE AS week, count(*) AS commits
            FROM stg_commits
            WHERE NOT is_bot
            GROUP BY ALL
        ),
        weekly AS (
            SELECT r.repo, w.week, coalesce(c.commits, 0) AS commits
            FROM (SELECT DISTINCT repo FROM stg_commits) r
            CROSS JOIN weeks w
            LEFT JOIN counts c USING (repo, week)
        ),
        fences AS (
            SELECT repo,
                   quantile_cont(commits, 0.75)
                       + {WEEKLY_IQR_MULTIPLIER} * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) AS fence
            FROM weekly
            GROUP BY repo
        )
        SELECT w.repo, w.week, w.commits, f.fence
        FROM weekly w
        JOIN fences f USING (repo)
        WHERE w.commits > f.fence
        ORDER BY w.repo, w.commits DESC
    """).df()
    log.info(
        f"Weekly-commit outliers (above Q3 + {WEEKLY_IQR_MULTIPLIER} x IQR per repo): {len(flagged)} weeks, "
        f"kept\n{flagged.to_string(index=False)}"
    )
    return flagged


def describe(con, log):
    """Per-repo medians and 90th percentiles of review and merge times, read from the marts. Returns the table."""
    stats = con.execute("""
        SELECT p.repo,
               round(median(p.hours_to_first_review), 1) AS median_first_review_h,
               round(quantile_cont(p.hours_to_first_review, 0.9), 1) AS p90_first_review_h,
               round(median(p.hours_to_merge), 1) AS median_merge_h,
               round(quantile_cont(p.hours_to_merge, 0.9), 1) AS p90_merge_h,
               round(h.commits_per_week, 1) AS commits_per_week,
               round(h.releases_per_year, 1) AS releases_per_year
        FROM fct_pull_requests p
        JOIN mart_health_inputs h USING (repo)
        WHERE NOT p.is_bot
        GROUP BY p.repo, h.commits_per_week, h.releases_per_year
        ORDER BY p.repo
    """).df()
    log.info(f"Descriptive statistics (human PRs; hours)\n{stats.to_string(index=False)}")
    return stats


@task
def validate():
    """Task 4: completeness checks (raise on any failure), outlier detection, and descriptive statistics."""
    log = TaskLogger("validate")
    # Not read_only: dbt (Task 3) keeps a connection to this file open in the same process, and DuckDB refuses a
    # second in-process connection with different settings. This task only runs SELECTs.
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("SET TimeZone = 'UTC'")

        failures = []
        for endpoint in TABLES:
            failures += check_landed_records(con, log, endpoint)
            failures += check_staging_counts(con, log, endpoint)
        failures += check_monthly_commits(con, log)
        if failures:
            raise RuntimeError("Validation failed:\n" + "\n".join(failures))
        log.info("All completeness checks passed")

        merge_outliers = flag_merge_outliers(con, log)
        commit_outliers = flag_commit_outliers(con, log)
        stats = describe(con, log)

    return {
        "merge_outliers": len(merge_outliers),
        "commit_outliers": len(commit_outliers),
        "stats": stats.to_dict("records"),
    }
