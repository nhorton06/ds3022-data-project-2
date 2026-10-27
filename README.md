# Open Source Package Health: GitHub Data Pipeline

This project is pulling data from three different GitHub repositories, each for a Python package.
It will be using package set A, which will include numpy, networkx, and statsmodels. It will be pulling these repositories
page by page, and comparing various metrics. The analysis window of the pages pulled will start from the beginning of
2024 and continue to the present.

## Pipeline overview

The first Prefect task will be extracting commits, pull requests, PR reviews, and releases in pages of 100, using the API on
GitHub to pull these individual pages. It will utilize error handling, retries and resuming, logging, and checkpointing.
The second task will be loading this data into DuckDB tables, formatting them, allowing for SQL queries, establishing keys.
The third task will be turning these loaded tables into a real build with staging models, marts, facts, and tests to make
sure that each of these steps is correct and data isn't facing crucial flaws like missing data or duplicates. The fourth
task will be validating the full data stream and its outputs utilizing Python. The fifth task will be the final model, 
providing health scores and ranking the various repositories based on these scores, relative to each other. The sixth task will
be plotting these metrics generated for comparison and easier visualization. The flow itself lives in main.py while each
individual task is in its own file under the pipeline folder, dbt is under the models folder, outputs in the plots folder, 
and log files per task being in the logs folder.

## Setup

To create an environment that will properly run this pipeline, you should be using Python 3.12, create a virtual environment
using `uv venv` and install dependencies from `requirements.txt`. Set the GitHub personal access token to the environment
variable name GH_PAT. The token is only read from the environment itself and is never written to the repository or the logs themselves.

Setup commands
```
uv venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt
export GH_PAT=<your token>
```

## Running the flow

The flow can be run by executing the command `python main.py` or `time python main.py` to include the time metrics for
running the flow itself on top of the outputs the pipeline already provides. The expected runtime for the first full run for
task 1 is a little over an hour (about 65 minutes or so, but it can slightly vary depending on when the flow is run during
the cycle of rate-limit resets of the hour and such). Task 1 writes to the `data/raw` folder and later tasks will write to
`packages.duckdb` and the `plots/` folder, and all tasks log to the `logs/` folder.

## Task 1: Extract

Task 1 uses 4 endpoints for each repo. The first endpoint is the commits, then the pulls, then pull request (PR) reviews, 
and then releases.
Commits are limited to the start of 2024 with `since=2024-01-01`, pull requests don't have a date filter, and are as such
sorted by most recently updated with paging stopping once a given page ends with a pull request updated before the window start (2024-01-01). Reviews are
pulled for every pull request created in that window whether or not it was actually merged. Releases are fetched in their entirety.
It uses the link header with rel="next" and
limits to 100 entries per page, saving each page as its own JSON file, which is then stored in
`data/raw/<owner_name>__<repo>/<endpoint>/page_<pg_number>.json`. Reviews
specifically are stored in `pr_reviews/pr_<number>_page_<pg_number>.json`. Record counts per endpoint per repo are logged
to the Prefect logger and `logs/extract.log`. For addressing the rate limit of a GitHub
account, the flow reads the response headers that show how many requests are left and when the limit resets
(`X-RateLimit-Remaining` and `X-RateLimit-Reset`, respectively) on every response. The flow will sleep when it reaches 10 or less requests remaining,
and then it will continue from there when the rate limit is reset. For other rate limits like 403/429, it waits the number
of seconds in the `Retry-After` header before retrying once again. For 5xx and network errors, Prefect retries the task up to 5 times
with a base backoff of 10 seconds, doubling with each retry. Other errors, such as a bad token or a 404, fail immediately, since
retrying wouldn't really help or do much for these. For resumability, pages that are already saved on the disk
are skipped, so the saved `next_url` will provide a checkpoint where to continue from in the case of interruptions and/or
errors. Pages are written to a temp (.tmp) file before being renamed so a crash never leaves pages unfinished.

### Record counts

| Repo | Commits | Pull requests | Reviews (PRs reviewed-for) | Releases |
| --- | --- | --- | --- | --- |
| numpy/numpy | 7,693 | 5,800 | 10,557 (5,477) | 149 |
| networkx/networkx | 898 | 1,400 | 4,168 (1,275) | 67 |
| statsmodels/statsmodels | 2,797 | 1,100 | 788 (994) | 32 |

### Interrupted and resumed run

I canceled/interrupted the run by pressing ctrl+c during the numpy review calls after about 300 pages had been saved.

Interrupted run:
```
16:44:03 | INFO  | No data/raw/: first run, fetching the full window
16:44:51 | INFO  | numpy/numpy commits: 7693 records, 77 pages fetched, 0 pages already on disk
16:46:04 | INFO  | numpy/numpy pulls: 5800 records, 58 pages fetched, 0 pages already on disk
16:46:05 | INFO  | numpy/numpy pr_reviews: 5477 PRs need reviews
16:47:30 | INFO  | numpy/numpy pr_reviews: 250/5477 PRs done
^C
16:47:51 | ERROR | Task run 'extract_reviews-544' - Crash detected! Execution was aborted by an interrupt signal.
```

Resumed run:
```
16:48:49 | INFO    | No _LOADED marker: resuming, pages already in data/raw/ will be skipped
16:48:50 | INFO    | numpy/numpy commits: 7693 records, 0 pages fetched, 77 pages already on disk
16:48:51 | INFO    | numpy/numpy pulls: 5800 records, 0 pages fetched, 58 pages already on disk
17:09:27 | WARNING | 10 requests left in this hour; sleeping 1307s until reset
17:36:35 | INFO    | numpy/numpy pr_reviews: 10557 records, 5166 pages fetched, 312 pages already on disk
17:50:02 | INFO    | Extract finished: 7501 API requests this run
```

### Rate limit and request counts

The resumed run hit the 5000 requests limit for the hour and slept until the reset. (This is shown in the warning above).
Following this sleep, it then resumed right where it left off until it finished the full flow.

| Run | API requests | Runtime |
| --- | --- | --- |
| Interrupted run | ~447 | 3m 52s |
| Resumed run | 7,501 | 61m 16s |
| Full first run total | ~7,950 | ~65 min |
| Incremental run | 105 | ~38s |

## Task 2: Load

Task 2's code is in the pipeline folder under `load.py`. It also includes the `load()` task in the flow, and it runs
directly after the `extract()` task in main.py. It just reads the data from the raw folder created in task 1, not calling
the API, only reading from the outputs from task 1. It creates raw tables, one for each endpoint, and creates a single
row per record. Each table has the `repo`, `id`, `updated_at`, `fetched_at`, `source_file`, `payload`, and a primary key using the
repo and id combined, as requested in the instructions. The `id` column stores the record's natural key (sha for
commits, number for pull requests, id for reviews/releases). An issue that came up is that only pull requests have a
real `updated_at` time, so the way around this was to use committer date for commits, `submitted_at` for reviews, and
`published_at` for releases.

For loading the data itself, I used DuckDB's `read_json()` command to read all the pages for an endpoint at the same
time, turning each page into one row per record using `unnest`. I also pulled out specific fields using DuckDB's `->>`
out of the JSON and cast them as specific data types with `::BIGINT` and `::TIMESTAMP`.

For idempotency, each table is created with `CREATE TABLE IF NOT EXISTS`, preventing duplicate table creation and so
later runs can reuse the same tables. Rows are added with `INSERT OR REPLACE` so if a row already exists with the unique
identifier, it'll replace it, and otherwise the row is just inserted into the table. This ensures no duplicates will be
written.

`_load_state` stores one row per repo per endpoint with the newest `updated_at` loaded, only writing after all the
tables have loaded, and then the `_LOADED` marker is made. On following runs, task 1 sees said marker and will delete
the raw data folder since it's all in DuckDB and use the watermark to fetch only new records. If loads crash, there's no
marker, so next runs resume and reload the files.

Before inserting the data, the QUALIFY statement keeps only the newest copy of a record if the same record appears more
than once in a batch.

### Loading the same files twice (idempotency)

Row counts from running Task 2 twice on the same landed files (`logs/load.log`):

| Table | Load 1 (before → after) | Load 2 (before → after) |
| --- | --- | --- |
| `raw_commits` | 0 → 11,388 | 11,388 → 11,388 |
| `raw_pull_requests` | 0 → 8,300 | 8,300 → 8,300 |
| `raw_pr_reviews` | 0 → 15,513 | 15,513 → 15,513 |
| `raw_releases` | 0 → 248 | 248 → 248 |

### Incremental run

A 2nd full run of the flow 2 days later properly found `_LOADED`, deleted the raw files, fetched only records created or
updated since the last run using the `_load_state` watermarks, taking 38 seconds for 105 API requests compared to 65
minutes for about 7950 requests from the first run.

| Table | Records fetched | Rows before → after |
| --- | --- | --- |
| `raw_commits` | 94 | 11,388 → 11,479 |
| `raw_pull_requests` | 300 | 8,300 → 8,345 |
| `raw_pr_reviews` | 52 | 15,513 → 15,531 |
| `raw_releases` | 248 | 248 → 248 |

Fewer rows were added than records fetched because most of the pull requests already existed and were updated in place.
Releases are always fetched in full, so all 248 were replaced with no new rows. 3 commits were the watermark commits
themselves (GitHub's `since` includes the boundary time), so the upsert replaced them rather than duplicating them.

```
14:14:44 | INFO | Found _LOADED: previous data is in DuckDB, deleting data/raw/ and fetching only new records
14:14:44 | INFO | numpy/numpy: commits since 2026-10-23T19:54:06Z, pulls since 2026-10-23T20:41:59Z
14:15:15 | INFO | Extract finished: 105 API requests this run
14:15:16 | INFO | raw_pull_requests: 300 records in files, 300 unique, rows before 8300 -> after 8345
```

## Task 3: dbt models and tests

Task 3 stores its code in the models, macros, and tests folders. It runs directly from the flow in the
`pipeline/transform.py` file, uses the `dbt_build()` task in the flow directly after `load()`, and uses PrefectDbtRunner
to run the `dbt build`. Any failed models or tests raise flags/errors and fail the flow. This is the only spot that dbt
actually runs. There aren't any retries on purpose because any fails for models or tests is an actual problem that
should be investigated, not just retried. For logging, it uses dbt's own logs under `logs/dbt.log`, logging the task's
per-model/test summary into the `transform.log` file and Prefect.

The staging layer uses views and marts use tables. The views are current without any real storage and marts are read
often by the tasks after this one. Raw tables are declared as a dbt source named raw. Staging has one model per raw
table (`stg_*`), `fct_pull_requests` and `fct_releases` add the flags and durations, and `mart_repo_monthly` and
`mart_health_inputs` hold the monthly counts and the health metrics for Task 5.

The bot rule uses SQL to check if the type of user is 'Bot' or if the user's login ends in '[bot]', and if so, it
declares it as a bot. This is applied to PR authors, reviewers, and commit authors. Missing values for user do not count
as bots, and this was written once as a macro so the rule will remain constant across models and such. These bots are
excluded from the marts and first-review timing as they're automated, so using them as metrics wouldn't make much sense.
The first review also excludes the pull request author's own reviews (since it's literally them who made it).

I also added new key columns because dbt uses single column key relationships/tests and some pull request numbers exist
in multiple repos, so it uses a combination of the repo and the number. I chose this over the `dbt_utils` package, which
would add a download step to every clean run. The timezone is manually set to UTC so there are no issues with timezones
being user-specific and changing data (otherwise events near a month boundary could be counted in the wrong month), and
run time is calculated by the maximum of the `fetched_at` in the raw tables so rebuilds with no new data gives the same
results (idempotency).

Open pull requests are kept in `fct_pull_requests` setting `is_open` to true and `merged_at` and `closed_at` staying
NULL because they're still actively being worked on. They're never dropped or given a fake close date, so
`hours_to_merge` and `hours_to_close` are NULL for them too. I used medians rather than means because the durations are
right-skewed (some pull requests have been open for years which pulls the mean way up above the typical pull request).
Since median skips NULL values, open pull requests drop out of the time-to-merge median and unreviewed ones drop out of
the time-to-first-review median, so they are instead addressed in `pct_open_stale` (open PRs) and
`pct_reviewed_within_7d` (unreviewed PRs count as not reviewed).

There are 24 tests, ultimately boiling down to a few simple things: checking for unique and non-null values in
identifier columns, checking that each review actually belongs to a pull request stored (`relationships`), checking
that pull requests and reviews have accepted values for their state (open, closed, etc.), and singular tests that are
more unique to the situation. The singular tests check that a pull request wasn't closed/merged before it was created
(since this is impossible) and that no review was submitted before the pull request was created (also impossible).
Each of these tests return the rows that don't pass said test, so passing the test means it returns zero rows.

### dbt build results

Latest flow run (`logs/transform.log` and dbt's summary line from `logs/dbt.log`), 8 models and 24 tests:

```
PASS    test  relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_
PASS    test  assert_pr_closed_after_created
PASS    test  assert_review_after_pr_created
Done. PASS=32 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=32
```

## Task 4: Validation

## Task 5: Health scoring and ranking

## Task 6: Plots

## Extended items
