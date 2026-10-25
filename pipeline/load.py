"""Task 2: load the landed JSON pages in data/raw/ into raw_* tables in packages.duckdb."""

from datetime import datetime, timezone

import duckdb
from prefect import task

from pipeline.common import DB_PATH, LOADED_MARKER, PROJECT_DIR, RAW_DIR, TaskLogger

# endpoint folder -> (table, id type, id JSON path, updated_at JSON path)
TABLES = {
    "commits": ("raw_commits", "VARCHAR", "$.sha", "$.commit.committer.date"),
    "pulls": ("raw_pull_requests", "BIGINT", "$.number", "$.updated_at"),
    "pr_reviews": ("raw_pr_reviews", "BIGINT", "$.id", "$.submitted_at"),
    "releases": ("raw_releases", "BIGINT", "$.id", "$.published_at"),
}


def load_endpoint(con, log, endpoint):
    """Upsert every landed page of one endpoint into its raw table. Returns the row count after loading."""
    table, id_type, id_path, updated_path = TABLES[endpoint]
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            repo VARCHAR,
            id {id_type},
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP,
            source_file VARCHAR,
            payload JSON,
            PRIMARY KEY (repo, id)
        )
    """)
    before = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    if not any(RAW_DIR.glob(f"*/{endpoint}/*.json")):
        log.info(f"{table}: no landed files, {before} rows unchanged")
        return before

    # One row per record: read every page, then unnest its data list
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE staged AS
        SELECT repo,
               (rec->>'{id_path}')::{id_type} AS id,
               (rec->>'{updated_path}')::TIMESTAMP AS updated_at,
               fetched_at::TIMESTAMP AS fetched_at,
               replace(filename, '{PROJECT_DIR}/', '') AS source_file,
               rec AS payload
        FROM (
            SELECT repo, fetched_at, filename, unnest(data) AS rec
            FROM read_json('{RAW_DIR}/*/{endpoint}/*.json',
                           columns = {{repo: 'VARCHAR', fetched_at: 'VARCHAR', data: 'JSON[]'}},
                           filename = true)
        )
    """)
    records, unique = con.execute("SELECT count(*), count(DISTINCT (repo, id)) FROM staged").fetchone()

    # Keep the newest copy of each record, then replace any existing row with the same (repo, id)
    con.execute(f"""
        INSERT OR REPLACE INTO {table}
        SELECT * FROM staged
        QUALIFY row_number() OVER (PARTITION BY repo, id ORDER BY fetched_at DESC) = 1
    """)
    after = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    log.info(f"{table}: {records} records in files, {unique} unique, rows before {before} -> after {after}")
    return after


def update_load_state(con, log):
    """Record the newest updated_at per repo and endpoint; Task 1 uses it as the next run's watermark."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS _load_state (
            repo VARCHAR,
            endpoint VARCHAR,
            max_updated_at TIMESTAMP,
            loaded_at TIMESTAMP,
            PRIMARY KEY (repo, endpoint)
        )
    """)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for endpoint, (table, *_) in TABLES.items():
        con.execute(f"""
            INSERT OR REPLACE INTO _load_state
            SELECT repo, '{endpoint}', max(updated_at), ? FROM {table} GROUP BY repo
        """, [loaded_at])
    for repo, endpoint, max_updated_at in con.execute(
        "SELECT repo, endpoint, max_updated_at FROM _load_state ORDER BY repo, endpoint"
    ).fetchall():
        log.info(f"_load_state: {repo} {endpoint} watermark {max_updated_at}")


@task(retries=2, retry_delay_seconds=10)
def load():
    """Task 2: upsert data/raw/ into raw tables, update _load_state, then create the _LOADED marker."""
    log = TaskLogger("load")
    counts = {}
    with duckdb.connect(str(DB_PATH)) as con:
        for endpoint, (table, *_) in TABLES.items():
            try:
                counts[table] = load_endpoint(con, log, endpoint)
            except duckdb.Error as exc:
                raise RuntimeError(f"Loading {endpoint} files into {table} failed: {exc}") from exc
        update_load_state(con, log)

    LOADED_MARKER.touch()
    log.info("All files loaded; created data/raw/_LOADED")
    return counts
