"""Task 1: pull commits, pull requests, PR reviews, and releases from GitHub into data/raw/."""

import json
import os
import shutil
import time
from datetime import datetime, timezone

import duckdb
import httpx
from prefect import task
from prefect.tasks import exponential_backoff

from pipeline.common import DB_PATH, LOADED_MARKER, RAW_DIR, TaskLogger

API = "https://api.github.com"
PER_PAGE = 100
RATE_LIMIT_BUFFER = 10  # sleep until reset once this few requests remain
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class RetryableHTTPError(Exception):
    """A failure that should clear up on its own (rate limit, 5xx, network)."""


def is_retryable(task, task_run, state):
    """Prefect retry condition: retry only transient errors, not bad tokens or 404s."""
    try:
        state.result()
    except RetryableHTTPError:
        return True
    except Exception:
        return False
    return False


def make_client():
    token = os.environ.get("GH_PAT")
    if not token:
        raise RuntimeError("GH_PAT environment variable is not set")
    return httpx.Client(
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        timeout=30,
    )


def github_get(client, url, log):
    """GET one page. Returns (json body, next page url or None)."""
    try:
        resp = client.get(url)
    except httpx.TransportError as exc:
        raise RetryableHTTPError(f"Network error for {url}: {exc}") from exc

    remaining = int(resp.headers.get("X-RateLimit-Remaining", RATE_LIMIT_BUFFER + 1))
    reset = int(resp.headers.get("X-RateLimit-Reset", time.time()))

    if resp.status_code in (403, 429):
        if remaining == 0:
            wait = reset - time.time() + 5
        elif "Retry-After" in resp.headers:
            wait = int(resp.headers["Retry-After"])
        elif "rate limit" in resp.text.lower():
            wait = 60  # GitHub says to wait at least a minute when no header is given
        else:
            raise RuntimeError(f"403 Forbidden for {url}: {resp.text[:200]}")
        log.warning(f"Rate limited ({resp.status_code}) on {url}; sleeping {wait:.0f}s before retry")
        time.sleep(max(wait, 0))
        raise RetryableHTTPError(f"Rate limited on {url}")

    if resp.status_code >= 500:
        raise RetryableHTTPError(f"Server error {resp.status_code} for {url}")
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}: {resp.text[:200]}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise RetryableHTTPError(f"Could not parse JSON from {url}") from exc

    if remaining <= RATE_LIMIT_BUFFER:
        wait = reset - time.time() + 5
        log.warning(f"{remaining} requests left in this hour; sleeping {wait:.0f}s until reset")
        time.sleep(max(wait, 0))

    return data, resp.links.get("next", {}).get("url")


def read_page(path):
    """Return a landed page, or None if it is missing or half-written."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def write_page(path, page):
    """Write to a .tmp file then rename, so a crash never leaves a partial page."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(page))
    tmp.replace(path)


def fetch_paginated(client, log, repo, endpoint, url, folder, prefix="", stop_before=None):
    """Follow rel="next" from url, landing one file per page and skipping pages already on disk.

    stop_before: for endpoints sorted by updated desc, stop once a page ends before this timestamp.
    Returns (records, pages fetched from the API, pages skipped from disk).
    """
    folder.mkdir(parents=True, exist_ok=True)
    page_num, records, fetched, skipped = 1, 0, 0, 0
    while url:
        path = folder / f"{prefix}page_{page_num:04d}.json"
        page = read_page(path)
        if page is None:
            data, next_url = github_get(client, url, log)
            page = {
                "repo": repo,
                "endpoint": endpoint,
                "url": url,
                "page": page_num,
                "fetched_at": datetime.now(timezone.utc).strftime(TS_FORMAT),
                "next_url": next_url,
                "data": data,
            }
            write_page(path, page)
            fetched += 1
        else:
            skipped += 1

        records += len(page["data"])
        url = page.get("next_url")
        if stop_before and page["data"] and page["data"][-1]["updated_at"] < stop_before:
            url = None
        page_num += 1
    return records, fetched, skipped


def repo_dir(repo):
    return RAW_DIR / repo.replace("/", "__")


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_condition_fn=is_retryable)
def extract_endpoint(repo, endpoint, since):
    """Fetch commits, pulls, or releases for one repo. Returns API requests made."""
    log = TaskLogger("extract")
    urls = {
        "commits": f"{API}/repos/{repo}/commits?since={since}&per_page={PER_PAGE}",
        "pulls": f"{API}/repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page={PER_PAGE}",
        "releases": f"{API}/repos/{repo}/releases?per_page={PER_PAGE}",
    }
    stop_before = since if endpoint == "pulls" else None
    with make_client() as client:
        records, fetched, skipped = fetch_paginated(
            client, log, repo, endpoint, urls[endpoint], repo_dir(repo) / endpoint, stop_before=stop_before
        )
    log.info(f"{repo} {endpoint}: {records} records, {fetched} pages fetched, {skipped} pages already on disk")
    return fetched


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_condition_fn=is_retryable)
def extract_reviews(repo, window_start, since):
    """Fetch reviews for every landed PR created in the window and updated since the watermark."""
    log = TaskLogger("extract")
    numbers = set()
    for path in sorted((repo_dir(repo) / "pulls").glob("page_*.json")):
        for pr in json.loads(path.read_text())["data"]:
            if pr["created_at"] >= window_start and pr["updated_at"] >= since:
                numbers.add(pr["number"])

    log.info(f"{repo} pr_reviews: {len(numbers)} PRs need reviews")
    folder = repo_dir(repo) / "pr_reviews"
    records, fetched, skipped = 0, 0, 0
    with make_client() as client:
        for i, number in enumerate(sorted(numbers), start=1):
            url = f"{API}/repos/{repo}/pulls/{number}/reviews?per_page={PER_PAGE}"
            r, f, s = fetch_paginated(client, log, repo, "pr_reviews", url, folder, prefix=f"pr_{number}_")
            records, fetched, skipped = records + r, fetched + f, skipped + s
            if i % 250 == 0:
                log.info(f"{repo} pr_reviews: {i}/{len(numbers)} PRs done")
    log.info(f"{repo} pr_reviews: {records} records, {fetched} pages fetched, {skipped} pages already on disk")
    return fetched


def load_watermarks():
    """Read {(repo, endpoint): newest updated_at} from _load_state, or {} before the first load."""
    if not DB_PATH.exists():
        return {}
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            rows = con.execute("SELECT repo, endpoint, max_updated_at FROM _load_state").fetchall()
    except duckdb.CatalogException:
        return {}
    return {(repo, endpoint): ts.strftime(TS_FORMAT) for repo, endpoint, ts in rows if ts}


@task
def extract(repos, window_start):
    """Task 1: land every page of every endpoint for each repo. Returns total API requests."""
    log = TaskLogger("extract")
    if LOADED_MARKER.exists():
        log.info("Found _LOADED: previous data is in DuckDB, deleting data/raw/ and fetching only new records")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        log.info("No _LOADED marker: resuming, pages already in data/raw/ will be skipped")
    else:
        log.info("No data/raw/: first run, fetching the full window")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    watermarks = load_watermarks()
    requests = 0
    for repo in repos:
        commits_since = watermarks.get((repo, "commits"), window_start)
        pulls_since = watermarks.get((repo, "pulls"), window_start)
        log.info(f"{repo}: commits since {commits_since}, pulls since {pulls_since}")
        requests += extract_endpoint(repo, "commits", commits_since)
        requests += extract_endpoint(repo, "pulls", pulls_since)
        requests += extract_reviews(repo, window_start, pulls_since)
        requests += extract_endpoint(repo, "releases", window_start)

    log.info(f"Extract finished: {requests} API requests this run")
    return requests
