# Open Source Package Health: GitHub Data Pipeline

<!--
One or two sentences: what this project does and which packages it compares.
Package set A: numpy/numpy, networkx/networkx, statsmodels/statsmodels.
Analysis window: 2024-01-01 through the time of each run.
Assignment instructions live in INSTRUCTIONS.md; the submission checklist is CHECKLIST.md.
-->
This project is pulling data from three different GitHub repositories, each for a Python package.
It will be using package set A, which will include numpy, networkx, and statsmodels. It will be pulling these repositories
page by page, and comparing various metrics. The analysis window of the pages pulled will start from the beginning of
2024 and continue to the present.

## Pipeline overview

<!--
Short description of the six Prefect tasks and what each one produces:
1 Extract (GitHub API -> data/raw/*.json), 2 Load (-> packages.duckdb raw_* tables),
3 dbt build (staging, fact, mart models + tests), 4 Validation (Python checks),
5 Model (health scores + ranking), 6 Plot (PNGs in plots/).
Optional: a list of where the code lives (main.py = the flow, pipeline/<task>.py = each task,
models/ = dbt, plots/ = output, logs/ = per-task log files).
-->
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

<!--
Steps someone would follow from a clean clone:
- Python version (3.12) and how to create the environment (uv venv + install from requirements.txt)
- Set the GitHub token: export GH_PAT=... (a classic PAT with no scopes is enough for public repos)
- Mention that the token is only read from the environment and never stored in the repo
-->
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

<!--
- Run command: python main.py  (optionally `time python main.py` to measure runtime)
- Expected runtime: full first run ~65 min (about 7,950 requests; one ~22 min rate-limit sleep
  because the 5,000/hour limit is exceeded). Incremental runs (after the first full run): ~38 s,
  105 requests when run two days later; scales with how much activity happened since the last run.
- Where output goes: data/raw/, packages.duckdb, logs/, plots/
-->
The flow can be run by executing the command `python main.py` or `time python main.py` to include the time metrics for
running the flow itself on top of the outputs the pipeline already provides. The expected runtime for the first full run for
task 1 is a little over an hour (about 65 minutes or so, but it can slightly vary depending on when the flow is run during
the cycle of rate-limit resets of the hour and such). Task 1 writes to the `data/raw` folder and later tasks will write to
`packages.duckdb` and the `plots/` folder, and all tasks log to the `logs/` folder.

## Task 1: Extract

<!--
How it works (in your own words):
- 4 endpoints per repo: commits (since=window start), pulls (sorted by updated desc, stop once a
  page ends before the window start), PR reviews (one call per PR created in the window), releases (all).
- Pagination: follows the Link header rel="next" with per_page=100; one JSON file per page in
  data/raw/<owner>__<repo>/<endpoint>/page_0001.json (reviews: pr_reviews/pr_<number>_page_0001.json).
- Rate limit: reads X-RateLimit-Remaining / X-RateLimit-Reset on every response, sleeps until reset
  when 10 or fewer requests remain. 403/429 honor Retry-After; 5xx and network errors are retried
  by Prefect (retries=5, exponential backoff 10/20/40/80/160 s).
- Resumability: pages already on disk are skipped; the saved next_url continues the chain.
  Pages are written to a .tmp file then renamed, so a crash never leaves a half-written page.

Evidence to include:
- Record counts table (from logs/extract.log):
    numpy:       7,693 commits | 5,800 PRs | 10,557 reviews (5,477 PRs) | 149 releases
    networkx:      898 commits | 1,400 PRs |  4,168 reviews (1,275 PRs) |  67 releases
    statsmodels: 2,797 commits | 1,100 PRs |    788 reviews (994 PRs)   |  32 releases
- Interrupted run log excerpt (Ctrl-C during numpy reviews, 312 review pages saved)
- Resumed run log excerpt ("No _LOADED marker: resuming", "0 pages fetched, 77 pages already on disk",
  "5166 pages fetched, 312 pages already on disk")
- Rate-limit sleep line: "10 requests left in this hour; sleeping 1307s until reset"
- Request counts: interrupted run ~447, resumed run 7,501 (full-run total ~7,950); incremental: after Task 2
-->

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

<!--
How it works (in your own words):
- Code: pipeline/load.py, task load(), runs right after extract() in main.py. Reads only data/raw/, never calls the API.
- Raw tables: one per endpoint (raw_commits, raw_pull_requests, raw_pr_reviews, raw_releases), one row per record.
  Columns: repo, id, updated_at, fetched_at (from the landing file), source_file (path of the page file), payload (full
  record as JSON). PRIMARY KEY (repo, id). Natural keys: commits -> sha, PRs -> number, reviews/releases -> id.
- updated_at mapping (only PRs have a real updated_at, so each endpoint uses its closest timestamp):
  PRs -> updated_at, commits -> commit.committer.date (same date GitHub's `since` filter uses),
  reviews -> submitted_at, releases -> published_at (NULL for drafts).
- Loading: DuckDB read_json() reads every page file for an endpoint at once; unnest(data) turns each page into one row per
  record; ->> pulls fields out of the JSON and :: casts them to BIGINT/TIMESTAMP. Payload is kept unchanged.
- Idempotency: INSERT OR REPLACE upserts on (repo, id), so loading the same files twice never adds duplicates.
  QUALIFY row_number() keeps only the newest copy if the same record appears twice in one batch (e.g. a PR that
  shifted between pages while paging).
- _load_state: one row per repo + endpoint with max(updated_at) and loaded_at. Written only after all four tables load,
  then data/raw/_LOADED is created. Next run, Task 1 sees _LOADED -> deletes data/raw/ and uses the _load_state
  watermarks as `since` / the pulls stop point. If a load crashes partway there is no marker, so the next run resumes
  and simply reloads (safe because of the upsert).
- Known limitation (optional): commits are filtered by committer date, so commits created before the watermark but
  pushed after it would be missed. Rare, since GitHub's merge/squash buttons create a new commit date.

Evidence to include:
- Loading twice on the same files (logs/load.log), rows before -> after:
    raw_commits:       load 1: 0 -> 11,388 | load 2: 11,388 -> 11,388
    raw_pull_requests: load 1: 0 -> 8,300  | load 2: 8,300 -> 8,300
    raw_pr_reviews:    load 1: 0 -> 15,513 | load 2: 15,513 -> 15,513
    raw_releases:      load 1: 0 -> 248    | load 2: 248 -> 248
- Incremental run (full flow, ~38 s, 105 API requests vs ~7,950 / ~65 min for the full run):
    raw_commits:        94 records fetched | 11,388 -> 11,479
    raw_pull_requests: 300 records fetched |  8,300 -> 8,345
    raw_pr_reviews:     52 records fetched | 15,513 -> 15,531
    raw_releases:      248 records fetched |    248 -> 248 (always fetched in full)
  Why fewer new rows than records fetched: most fetched PRs already existed and were updated in place; 3 commits
  were the watermark commits themselves (GitHub's `since` includes the boundary), replaced rather than duplicated.
- Log lines worth quoting:
    Found _LOADED: previous data is in DuckDB, deleting data/raw/ and fetching only new records
    numpy/numpy: commits since 2026-10-23T19:54:06Z, pulls since 2026-10-23T20:41:59Z
    Extract finished: 105 API requests this run
    raw_pull_requests: 300 records in files, 300 unique, rows before 8300 -> after 8345
-->
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

<!--
Fill in after Task 3: the bot rule you used, how medians treat PRs still open at run time,
the models and tests you built.
-->

## Task 4: Validation

<!--
Fill in after Task 4: outlier cutoffs, which records were flagged, and whether you kept,
capped, or dropped them and why. Descriptive statistics summary.
-->

## Task 5: Health scoring and ranking

<!--
Fill in after Task 5: each metric's definition and direction (higher or lower is healthier),
at least 4 metrics across 3 dimensions, your weights and the reasoning behind them, final ranking table.
-->

## Task 6: Plots

<!--
Fill in after Task 6: embed each PNG from plots/ and write a one-paragraph interpretation of each.
-->

## Extended items

<!--
Fill in only for the extended items you attempt (GraphQL, concurrent fetching,
scheduled deployment, API odometer).
-->
