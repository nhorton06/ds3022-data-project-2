"""Task 1: pull commits, pull requests, PR reviews, and releases from GitHub into data/raw/."""

import json
import os
import shutil
import threading
import time
from datetime import datetime, timezone

import duckdb
import httpx
from prefect import task
from prefect.tasks import exponential_backoff

from pipeline.common import DB_PATH, LOADED_MARKER, RAW_DIR, TaskLogger

API = "https://api.github.com"
GRAPHQL_URL = f"{API}/graphql"
PER_PAGE = 100
RATE_LIMIT_BUFFER = 10  # sleep until reset once this few requests (or GraphQL points) remain
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# API odometer: every request sent to GitHub in this flow run, counted in send() (the one function all requests
# go through), so rate-limited, failed, and retried requests count too. Reset at the start of extract().
# The lock keeps the counts right when repos are fetched in parallel threads.
odometer = {"rest": 0, "graphql": 0, "graphql_points": 0}
odometer_lock = threading.Lock()


def count(key, amount=1):
    with odometer_lock:
        odometer[key] += amount


class RateLimiter:
    """One shared limiter per API for all parallel threads: when any thread sees the budget running low (or gets
    rate limited), it sets a pause, and every thread waits for it before its next request."""

    def __init__(self):
        self.lock = threading.Lock()
        self.paused_until = 0.0

    def pause_until(self, timestamp):
        with self.lock:
            self.paused_until = max(self.paused_until, timestamp)

    def wait(self, log, api):
        with self.lock:
            wait = self.paused_until - time.time()
        if wait > 0:
            log.warning(f"{api} rate limit pause: sleeping {wait:.0f}s")
            time.sleep(wait)


limiters = {"rest": RateLimiter(), "graphql": RateLimiter()}

# One query returns 100 PRs with up to 100 reviews each (replacing ~100 REST review calls), sorted by most
# recently updated so paging can stop at the window start / watermark like the REST version.
PULLS_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: 100, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number state createdAt updatedAt closedAt mergedAt
        author { __typename login }
        reviews(first: 100) {
          pageInfo { hasNextPage endCursor }
          nodes { databaseId state submittedAt author { __typename login } }
        }
      }
    }
  }
}
"""

# Follow-up query for the rare PR with more than 100 reviews
MORE_REVIEWS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { databaseId state submittedAt author { __typename login } }
      }
    }
  }
}
"""


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


GH_SECRET_BLOCK = os.environ.get("GH_SECRET_BLOCK", "dxx6gu-pat")


def get_token():
    """The GitHub token: the GH_PAT environment variable locally, or the Prefect Secret block on the deployment."""
    token = os.environ.get("GH_PAT")
    if token:
        return token
    try:
        from prefect.blocks.system import Secret

        return Secret.load(GH_SECRET_BLOCK).get()
    except Exception as exc:
        raise RuntimeError(f"GH_PAT is not set and Prefect Secret block '{GH_SECRET_BLOCK}' could not be loaded") from exc


def make_client():
    token = get_token()
    return httpx.Client(
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        timeout=30,
    )


def send(client, method, url, log, api, **kwargs):
    """Send one request to the REST or GraphQL API and apply every rate-limit and error rule. Returns (resp, body)."""
    limiters[api].wait(log, api)  # respect a pause set by any thread
    count(api)  # counted before the response is checked, so failures count too
    try:
        resp = client.request(method, url, **kwargs)
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
        log.warning(f"Rate limited ({resp.status_code}) on {url}; pausing all {api} requests {wait:.0f}s, then retrying")
        limiters[api].pause_until(time.time() + wait)
        limiters[api].wait(log, api)
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
        log.warning(f"{remaining} {api} requests/points left in this hour; pausing all {api} requests until reset")
        limiters[api].pause_until(reset + 5)

    return resp, data


def github_get(client, url, log):
    """GET one REST page. Returns (json body, next page url or None)."""
    resp, data = send(client, "GET", url, log, "rest")
    return data, resp.links.get("next", {}).get("url")


def graphql_post(client, query, variables, log):
    """Run one GraphQL query and track the point budget from its rateLimit field. Returns the data object."""
    _, body = send(client, "POST", GRAPHQL_URL, log, "graphql", json={"query": query, "variables": variables})
    errors = body.get("errors") or []
    if any(error.get("type") == "RATE_LIMITED" for error in errors):
        log.warning("GraphQL rate limited; pausing all GraphQL requests 60s, then retrying")
        limiters["graphql"].pause_until(time.time() + 60)
        limiters["graphql"].wait(log, "graphql")
        raise RetryableHTTPError("GraphQL rate limited")
    if errors:
        raise RuntimeError(f"GraphQL errors for {variables}: {[e.get('message') for e in errors]}")

    rate = body["data"]["rateLimit"]
    count("graphql_points", rate["cost"])
    if rate["remaining"] <= RATE_LIMIT_BUFFER:
        reset = datetime.strptime(rate["resetAt"], TS_FORMAT).replace(tzinfo=timezone.utc).timestamp()
        log.warning(f"{rate['remaining']} GraphQL points left; pausing all GraphQL requests until reset")
        limiters["graphql"].pause_until(reset + 5)
    return body["data"]


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


def rest_user(author):
    """GraphQL author -> REST user shape. REST bot logins end in [bot]; GraphQL's don't, so add it back."""
    if author is None:
        return None
    is_bot = author["__typename"] == "Bot"
    return {"login": author["login"] + ("[bot]" if is_bot else ""), "type": "Bot" if is_bot else "User"}


def rest_pr(node):
    """GraphQL PR -> the REST fields the pipeline uses, so Task 2 and dbt work unchanged."""
    return {
        "number": node["number"],
        "state": "open" if node["state"] == "OPEN" else "closed",  # REST has no MERGED state; merged_at says it
        "created_at": node["createdAt"],
        "updated_at": node["updatedAt"],
        "closed_at": node["closedAt"],
        "merged_at": node["mergedAt"],
        "user": rest_user(node["author"]),
    }


def rest_reviews(nodes, repo, number):
    """GraphQL reviews -> REST review shape, including the pull_request_url staging parses the PR number from."""
    return [
        {
            "id": node["databaseId"],
            "state": node["state"],
            "submitted_at": node["submittedAt"],
            "user": rest_user(node["author"]),
            "pull_request_url": f"{API}/repos/{repo}/pulls/{number}",
        }
        for node in nodes
        if node["databaseId"] is not None
    ]


def fetch_more_reviews(client, log, repo, number, cursor, folder):
    """Page through reviews past the first 100 for one PR, landing pr_<number>_page_0002.json onward."""
    owner, name = repo.split("/")
    page_num = 2
    while cursor:
        data = graphql_post(client, MORE_REVIEWS_QUERY, {"owner": owner, "name": name, "number": number,
                                                         "cursor": cursor}, log)
        conn = data["repository"]["pullRequest"]["reviews"]
        cursor = conn["pageInfo"]["endCursor"] if conn["pageInfo"]["hasNextPage"] else None
        write_page(folder / f"pr_{number}_page_{page_num:04d}.json", {
            "repo": repo, "endpoint": "pr_reviews", "api": "graphql", "url": GRAPHQL_URL, "page": page_num,
            "fetched_at": datetime.now(timezone.utc).strftime(TS_FORMAT), "data": rest_reviews(conn["nodes"], repo, number),
        })
        page_num += 1


def fetch_pulls_graphql(client, log, repo, stop_before):
    """Cursor-paginate PRs (newest updated first) with their reviews, landing one pulls file and one reviews file
    per response page. Skips pages already on disk and stops once a page ends before stop_before.
    Returns (PRs, reviews, queries sent, pages skipped from disk).
    """
    owner, name = repo.split("/")
    pulls_dir, reviews_dir = repo_dir(repo) / "pulls", repo_dir(repo) / "pr_reviews"
    pulls_dir.mkdir(parents=True, exist_ok=True)
    reviews_dir.mkdir(parents=True, exist_ok=True)
    cursor, page_num, prs, reviews, fetched, skipped = None, 1, 0, 0, 0, 0
    while True:
        pulls_path = pulls_dir / f"page_{page_num:04d}.json"
        reviews_path = reviews_dir / f"page_{page_num:04d}.json"
        page, review_page = read_page(pulls_path), read_page(reviews_path)
        if page is None or review_page is None or page.get("api") != "graphql":
            data = graphql_post(client, PULLS_QUERY, {"owner": owner, "name": name, "cursor": cursor}, log)
            conn = data["repository"]["pullRequests"]
            review_records = []
            for node in conn["nodes"]:
                review_records += rest_reviews(node["reviews"]["nodes"], repo, node["number"])
                if node["reviews"]["pageInfo"]["hasNextPage"]:
                    fetch_more_reviews(client, log, repo, node["number"], node["reviews"]["pageInfo"]["endCursor"],
                                       reviews_dir)
            meta = {"repo": repo, "api": "graphql", "url": GRAPHQL_URL, "cursor": cursor, "page": page_num,
                    "fetched_at": datetime.now(timezone.utc).strftime(TS_FORMAT)}
            next_cursor = conn["pageInfo"]["endCursor"] if conn["pageInfo"]["hasNextPage"] else None
            review_page = {**meta, "endpoint": "pr_reviews", "data": review_records}
            page = {**meta, "endpoint": "pulls", "next_cursor": next_cursor, "data": [rest_pr(n) for n in conn["nodes"]]}
            write_page(reviews_path, review_page)  # pulls file last: if it exists, its reviews page does too
            write_page(pulls_path, page)
            fetched += 1
        else:
            skipped += 1

        prs += len(page["data"])
        reviews += len(review_page["data"])
        cursor = page["next_cursor"]
        if cursor is None or (stop_before and page["data"] and page["data"][-1]["updated_at"] < stop_before):
            break
        page_num += 1
    return prs, reviews, fetched, skipped


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_condition_fn=is_retryable)
def extract_endpoint(repo, endpoint, since):
    """Fetch commits, pulls, or releases for one repo. Returns pages fetched from the API."""
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


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_condition_fn=is_retryable)
def extract_pulls_graphql(repo, since):
    """Fetch PRs and their reviews for one repo through GraphQL. Returns queries sent."""
    log = TaskLogger("extract")
    with make_client() as client:
        prs, reviews, fetched, skipped = fetch_pulls_graphql(client, log, repo, since)
    log.info(f"{repo} pulls + pr_reviews (GraphQL): {prs} PRs, {reviews} reviews, "
             f"{fetched} queries, {skipped} pages already on disk")
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
def extract_repo(repo, window_start, commits_since, pulls_since, pr_source):
    """All endpoints for one repo, in order (reviews need the PR pages). Runs in parallel with the other repos."""
    log = TaskLogger("extract")
    log.info(f"{repo}: commits since {commits_since}, pulls since {pulls_since}")
    pages = extract_endpoint(repo, "commits", commits_since)
    if pr_source == "graphql":
        pages += extract_pulls_graphql(repo, pulls_since)
    else:
        pages += extract_endpoint(repo, "pulls", pulls_since)
        pages += extract_reviews(repo, window_start, pulls_since)
    pages += extract_endpoint(repo, "releases", window_start)
    return pages


@task
def extract(repos, window_start, pr_source="graphql"):
    """Task 1: land every page of every endpoint for each repo. PRs and reviews come from GraphQL by default
    (pr_source="rest" uses the original REST calls). Returns the API odometer (total requests)."""
    odometer.update(rest=0, graphql=0, graphql_points=0)
    started = time.monotonic()
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
    # Concurrent fetching: submit every repo at once (Prefect runs them in parallel threads); the shared
    # limiters and the odometer lock keep the combined usage under the quota and the counts right.
    futures = [
        extract_repo.submit(repo, window_start, watermarks.get((repo, "commits"), window_start),
                            watermarks.get((repo, "pulls"), window_start), pr_source)
        for repo in repos
    ]
    pages = sum(future.result() for future in futures)

    total = odometer["rest"] + odometer["graphql"]
    log.info(f"Extract finished ({pr_source} for PRs/reviews): {pages} pages fetched in {time.monotonic() - started:.0f}s")
    log.info(f"API odometer: {total} requests this flow run (REST {odometer['rest']}, GraphQL {odometer['graphql']}, "
             f"GraphQL points {odometer['graphql_points']}; {pages} pages saved)")
    return total
