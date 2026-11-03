# Prefect Deployments

All previous in-class examples of Prefect flows have run (executed) on student laptops, while their logging and output has been visible through the SDS Prefect server https://sds-prefect.pods.uvarc.io/.

However, an advanced feature of Prefect is that it can run scheduled or "detached" flows on the server itself. 

Here's how to set that up and enable it:

## Blocks / Secrets

1. Keep your entire flow script and all related files in a single repository. You will need the repo URL as part of the deployment.
2. Any private ENV variables your script depends on (such as your `GH_PAT`) must first be published to the Prefect Server as "Blocks" (i.e. a feature that can store secret variables you can call within your code).
   1. Go to https://sds-prefect.pods.uvarc.io/v2/blocks
   2. Select "+ New storage block"
   3. Select "Secret" (a generic secret), then "Create"
   4. Give the block a unique **name**, using your computing ID, such as `nem2p-pat` (lowercase letters, numbers, and dashes only). The value should be of the "password" type and contain the actual value of your PAT.
   5. Click "Save".
3. To use a Prefect Block within your flow, use this syntax:

    ```
    from prefect.blocks.system import Secret

    my_secret = Secret.load("nem2p-pat")

    # Access the stored secret in a reusable object
    secret = my_secret.get()
    ```

## Deployments

See the [reference example](https://gist.github.com/nmagee/177f916afefbc7ae5bcfa7e213ca5dd9) for all details and setup, but the crux is found in the `__main__` handler at the bottom:

```
if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    <flow_function_name>.from_source(
        source=GIT_URL,
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        name="package-health",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
```

In the reference example a GitHub Gist repository was used, but any normal GitHub repo will work, using the `https://` prefix with the `.git` suffix.

### Private repositories

If your repo is private, the Prefect worker needs your PAT to clone it. A PAT with no scopes can only read public repositories, so generate a new one that can also read private repositories, and store it in your Secret block in place of the old one:

1. Go to GitHub → Settings → Developer settings → Personal access tokens → **Tokens (classic)** → "Generate new token (classic)".
2. Set an expiration that lasts past the end of the semester (60 days).
3. Check the **`repo`** scope. No other scopes are needed.
4. Copy the token and update the value of your Secret block (e.g. `nem2p-pat`).

A fine-grained token also works if you prefer narrower access: grant it access to your project repository with **Contents: Read-only** permission.

Instead of passing `GIT_URL` as a plain string, wrap it in a `GitRepository` object and pass your Secret block as the credential:

```
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# be sure to use your PAT block below
if __name__ == "__main__":
    <flow_function_name>.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load("nem2p-pat")},
        ),
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        ...  # same deploy() arguments as above
    )
```

Pass the loaded `Secret` block itself, not `Secret.load(...).get()`. The deployment then stores only a reference to the block, and the worker fetches the token at run time, so the PAT is never written into the deployment's configuration.

The entrypoint should be the filename+primary flow function name, like this:

    main.py:package_health_flow

Keep the following values as written above:

- `WORK_POOL = "uvasds-k8s-pool"`
- `job_variables` and its values

To publish your deployment once written (and the entire code including your GIT_URL is correct and its content up-to-date), run this command from your project, pointing at the flow's entrypoint:

    prefect deploy ./main.py:<flow_function_name> \
        --name nem2p-deployment \
        --pool uvasds-k8s-pool

## Test your deployment

You should see your deployment listed in https://sds-prefect.pods.uvarc.io/v2/deployments

Select it to see details and any previous runs. Use the blue "Run" button in the upper-right to trigger a run, which you can then follow like any other run.

**For this project, trigger your deployment manually once.** The first scheduled run may not fall before the due date, so a manual run from the "Run" button is what you submit. Copy its log into `DEPLOYMENTS.md` along with your deployment command.

See the [NYC-Taxi-Monthly](https://sds-prefect.pods.uvarc.io/v2/deployments/deployment/987975de-1d75-48d3-810e-251d9e077b16?tab=Runs) sample deployment and try running it for yourself.

The savvy BSDS student will recognize that some outputs such as plots will not be available since they are built into the remote environment executing the code. This is okay ;^)

(In production the plots would be copied off to remote storage like S3, etc.)

---

# My deployment (package set A)

Deployment `package-health/dxx6gu-deployment` on work pool `uvasds-k8s-pool`, scheduled for midnight Eastern on the 28th of every month (`Cron("0 0 28 * *", timezone="America/New_York")`). The code is in [`deploy.py`](deploy.py). It clones this private repo with the `dxx6gu-pat` Secret block (a token with the `repo` scope), and the flow reads its GitHub token from that same block on the server (`GH_PAT` is used locally). Because the worker image does not include dbt and the other packages, `job_variables` adds `EXTRA_PIP_PACKAGES` with the pinned `requirements.txt`; the course's CPU and memory values are unchanged.

## Deployment command

```
python deploy.py
```

Output:

```
Successfully created/updated all deployments!
 package-health/dxx6gu-deployment | applied
You can also run your flow via the Prefect UI: https://sds-prefect.pods.uvarc.io/deployments/deployment/7cc2967f-6e0b-4df7-99cf-635de9b5db08
```

## Manually triggered run (Prefect UI, Run button)

Flow run `monumental-nyala`, Nov 3, 2026, state **Completed** (about 3 minutes). Each run on the server starts from an empty working folder, so it is always a full first run (no `data/raw/` or `packages.duckdb` from earlier runs); with GraphQL and concurrent fetching that takes 214 requests instead of about 7,950.

```
# flow run monumental-nyala (f4a9054e-9efa-4d3f-93f5-800b199280bd) state=Completed start=2026-11-03 20:20:09.784711+00:00 end=2026-11-03 20:23:15.783855+00:00
20:19:39 | 20 | prefect.flow_runs.worker | Worker 'KubernetesWorker 9b5579bd-3a84-4b0b-a05a-28e2a22d6e11' submitting flow run 'f4a9054e-9efa-4d3f-93f5-800b199280bd'
20:19:39 | 20 | prefect.flow_runs.worker | Creating Kubernetes job...
20:19:39 | 20 | prefect.flow_runs.worker | Kubernetes job 'monumental-nyala-qmc8c' created in namespace 'uvasds-services'
20:19:39 | 20 | prefect.flow_runs.worker | Flow run 'monumental-nyala' submitted to infrastructure
20:19:44 | 20 | prefect.flow_runs | Running 1 deployment pull step(s)
20:19:44 | 20 | prefect.flow_runs | Executing deployment step: git_clone
20:20:03 | 20 | prefect.flow_runs | Deployment step 'git_clone' completed successfully
20:20:03 | 20 | prefect.flow_runs | All deployment steps completed successfully
20:20:09 | 20 | prefect.flow_runs | Beginning flow run 'monumental-nyala' for flow 'package-health'
20:20:09 | 20 | prefect.task_runs | No data/raw/: first run, fetching the full window
20:20:09 | 20 | prefect.task_runs | numpy/numpy: commits since 2024-01-01T00:00:00Z, pulls since 2024-01-01T00:00:00Z
20:20:09 | 20 | prefect.task_runs | networkx/networkx: commits since 2024-01-01T00:00:00Z, pulls since 2024-01-01T00:00:00Z
20:20:09 | 20 | prefect.task_runs | statsmodels/statsmodels: commits since 2024-01-01T00:00:00Z, pulls since 2024-01-01T00:00:00Z
20:20:16 | 20 | prefect.task_runs | networkx/networkx commits: 902 records, 10 pages fetched, 0 pages already on disk
20:20:16 | 20 | prefect.task_runs | Finished in state Completed()
20:20:33 | 20 | prefect.task_runs | statsmodels/statsmodels commits: 3365 records, 34 pages fetched, 0 pages already on disk
20:20:33 | 20 | prefect.task_runs | Finished in state Completed()
20:20:47 | 20 | prefect.task_runs | networkx/networkx pulls + pr_reviews (GraphQL): 1500 PRs, 5012 reviews, 15 queries, 0 pages already on disk
20:20:47 | 20 | prefect.task_runs | Finished in state Completed()
20:20:47 | 20 | prefect.task_runs | networkx/networkx releases: 67 records, 1 pages fetched, 0 pages already on disk
20:20:47 | 20 | prefect.task_runs | Finished in state Completed()
20:20:47 | 20 | prefect.task_runs | Finished in state Completed()
20:20:51 | 20 | prefect.task_runs | statsmodels/statsmodels pulls + pr_reviews (GraphQL): 1200 PRs, 992 reviews, 12 queries, 0 pages already on disk
20:20:51 | 20 | prefect.task_runs | Finished in state Completed()
20:20:51 | 20 | prefect.task_runs | statsmodels/statsmodels releases: 32 records, 1 pages fetched, 0 pages already on disk
20:20:51 | 20 | prefect.task_runs | Finished in state Completed()
20:20:51 | 20 | prefect.task_runs | Finished in state Completed()
20:21:07 | 20 | prefect.task_runs | numpy/numpy commits: 7736 records, 78 pages fetched, 0 pages already on disk
20:21:07 | 20 | prefect.task_runs | Finished in state Completed()
20:22:53 | 20 | prefect.task_runs | numpy/numpy pulls + pr_reviews (GraphQL): 5900 PRs, 12785 reviews, 59 queries, 0 pages already on disk
20:22:53 | 20 | prefect.task_runs | Finished in state Completed()
20:22:54 | 20 | prefect.task_runs | numpy/numpy releases: 149 records, 2 pages fetched, 0 pages already on disk
20:22:54 | 20 | prefect.task_runs | Finished in state Completed()
20:22:54 | 20 | prefect.task_runs | Finished in state Completed()
20:22:54 | 20 | prefect.task_runs | Extract finished (graphql for PRs/reviews): 212 pages fetched in 165s
20:22:54 | 20 | prefect.task_runs | API odometer: 214 requests this flow run (REST 126, GraphQL 88, GraphQL points 88; 212 pages saved)
20:22:54 | 20 | prefect.task_runs | Finished in state Completed()
20:22:56 | 20 | prefect.task_runs | raw_commits: 12003 records in files, 12003 unique, rows before 0 -> after 12003
20:22:56 | 20 | prefect.task_runs | raw_pull_requests: 8600 records in files, 8600 unique, rows before 0 -> after 8600
20:22:57 | 20 | prefect.task_runs | raw_pr_reviews: 18829 records in files, 18829 unique, rows before 0 -> after 18829
20:22:57 | 20 | prefect.task_runs | raw_releases: 248 records in files, 248 unique, rows before 0 -> after 248
20:22:57 | 20 | prefect.task_runs | _load_state: networkx/networkx commits watermark 2026-11-03 00:27:50
20:22:57 | 20 | prefect.task_runs | _load_state: networkx/networkx pr_reviews watermark 2026-11-03 17:52:02
20:22:57 | 20 | prefect.task_runs | _load_state: networkx/networkx pulls watermark 2026-11-03 18:49:07
20:22:57 | 20 | prefect.task_runs | _load_state: networkx/networkx releases watermark 2026-09-21 16:46:13
20:22:57 | 20 | prefect.task_runs | _load_state: numpy/numpy commits watermark 2026-11-03 19:40:37
20:22:57 | 20 | prefect.task_runs | _load_state: numpy/numpy pr_reviews watermark 2026-11-03 19:50:28
20:22:57 | 20 | prefect.task_runs | _load_state: numpy/numpy pulls watermark 2026-11-03 20:14:49
20:22:57 | 20 | prefect.task_runs | _load_state: numpy/numpy releases watermark 2026-10-10 21:30:10
20:22:57 | 20 | prefect.task_runs | _load_state: statsmodels/statsmodels commits watermark 2026-11-03 13:54:56
20:22:57 | 20 | prefect.task_runs | _load_state: statsmodels/statsmodels pr_reviews watermark 2026-11-01 12:23:38
20:22:57 | 20 | prefect.task_runs | _load_state: statsmodels/statsmodels pulls watermark 2026-11-03 14:00:19
20:22:57 | 20 | prefect.task_runs | _load_state: statsmodels/statsmodels releases watermark 2026-08-27 06:44:51
20:22:57 | 20 | prefect.task_runs | All files loaded; created data/raw/_LOADED
20:22:57 | 20 | prefect.task_runs | Finished in state Completed()
20:22:58 | 20 | prefect.task_runs | Running with dbt=1.12.6
20:22:58 | 20 | prefect.task_runs | Registered adapter: duckdb=1.11.0
20:22:58 | 20 | prefect.task_runs | Unable to do partial parsing because saved manifest not found. Starting full parse.
20:23:02 | 20 | prefect.task_runs | Found 8 models, 24 data tests, 4 sources, 502 macros
20:23:03 | 20 | prefect.task_runs | Concurrency: 1 threads (target='dev')
20:23:03 | 20 | prefect.task_runs | 1 of 32 START sql view model main.stg_commits .................................. [RUN]
20:23:03 | 20 | prefect.task_runs | 1 of 32 OK created sql view model main.stg_commits ............................. [OK in 0.14s]
20:23:03 | 20 | prefect.task_runs | 2 of 32 START sql view model main.stg_pr_reviews ............................... [RUN]
20:23:03 | 20 | prefect.task_runs | 2 of 32 OK created sql view model main.stg_pr_reviews .......................... [OK in 0.04s]
20:23:03 | 20 | prefect.task_runs | 3 of 32 START sql view model main.stg_pull_requests ............................ [RUN]
20:23:03 | 20 | prefect.task_runs | 3 of 32 OK created sql view model main.stg_pull_requests ....................... [OK in 0.14s]
20:23:03 | 20 | prefect.task_runs | 4 of 32 START sql view model main.stg_releases ................................. [RUN]
20:23:03 | 20 | prefect.task_runs | 4 of 32 OK created sql view model main.stg_releases ............................ [OK in 0.09s]
20:23:03 | 20 | prefect.task_runs | 5 of 32 START test not_null_stg_commits_commit_key ............................. [RUN]
20:23:03 | 20 | prefect.task_runs | 5 of 32 PASS not_null_stg_commits_commit_key ................................... [PASS in 0.17s]
20:23:03 | 20 | prefect.task_runs | 6 of 32 START test unique_stg_commits_commit_key ............................... [RUN]
20:23:03 | 20 | prefect.task_runs | 6 of 32 PASS unique_stg_commits_commit_key ..................................... [PASS in 0.14s]
20:23:03 | 20 | prefect.task_runs | 7 of 32 START test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [RUN]
20:23:03 | 20 | prefect.task_runs | 7 of 32 PASS accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [PASS in 0.14s]
20:23:03 | 20 | prefect.task_runs | 8 of 32 START test not_null_stg_pr_reviews_pr_key .............................. [RUN]
20:23:03 | 20 | prefect.task_runs | 8 of 32 PASS not_null_stg_pr_reviews_pr_key .................................... [PASS in 0.14s]
20:23:03 | 20 | prefect.task_runs | 9 of 32 START test not_null_stg_pr_reviews_review_key .......................... [RUN]
20:23:03 | 20 | prefect.task_runs | 9 of 32 PASS not_null_stg_pr_reviews_review_key ................................ [PASS in 0.12s]
20:23:03 | 20 | prefect.task_runs | 10 of 32 START test unique_stg_pr_reviews_review_key ........................... [RUN]
20:23:04 | 20 | prefect.task_runs | 10 of 32 PASS unique_stg_pr_reviews_review_key ................................. [PASS in 0.15s]
20:23:04 | 20 | prefect.task_runs | 11 of 32 START test accepted_values_stg_pull_requests_state__open__closed ...... [RUN]
20:23:04 | 20 | prefect.task_runs | 11 of 32 PASS accepted_values_stg_pull_requests_state__open__closed ............ [PASS in 0.17s]
20:23:04 | 20 | prefect.task_runs | 12 of 32 START test assert_pr_closed_after_created ............................. [RUN]
20:23:04 | 20 | prefect.task_runs | 12 of 32 PASS assert_pr_closed_after_created ................................... [PASS in 0.16s]
20:23:04 | 20 | prefect.task_runs | 13 of 32 START test assert_review_after_pr_created ............................. [RUN]
20:23:04 | 20 | prefect.task_runs | 13 of 32 PASS assert_review_after_pr_created ................................... [PASS in 0.31s]
20:23:04 | 20 | prefect.task_runs | 14 of 32 START test not_null_stg_pull_requests_pr_key .......................... [RUN]
20:23:04 | 20 | prefect.task_runs | 14 of 32 PASS not_null_stg_pull_requests_pr_key ................................ [PASS in 0.15s]
20:23:04 | 20 | prefect.task_runs | 15 of 32 START test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [RUN]
20:23:05 | 20 | prefect.task_runs | 15 of 32 PASS relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [PASS in 0.28s]
20:23:05 | 20 | prefect.task_runs | 16 of 32 START test unique_stg_pull_requests_pr_key ............................ [RUN]
20:23:05 | 20 | prefect.task_runs | 16 of 32 PASS unique_stg_pull_requests_pr_key .................................. [PASS in 0.26s]
20:23:05 | 20 | prefect.task_runs | 17 of 32 START test not_null_stg_releases_release_key .......................... [RUN]
20:23:05 | 20 | prefect.task_runs | Finished in state Completed()
20:23:05 | 20 | prefect.task_runs | 17 of 32 PASS not_null_stg_releases_release_key ................................ [PASS in 0.26s]
20:23:05 | 20 | prefect.task_runs | 18 of 32 START test unique_stg_releases_release_key ............................ [RUN]
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:06 | 20 | prefect.task_runs | 18 of 32 PASS unique_stg_releases_release_key .................................. [PASS in 0.29s]
20:23:06 | 20 | prefect.task_runs | 19 of 32 START sql table model main.fct_pull_requests .......................... [RUN]
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:06 | 20 | prefect.task_runs | 19 of 32 OK created sql table model main.fct_pull_requests ..................... [OK in 0.67s]
20:23:06 | 20 | prefect.task_runs | 20 of 32 START sql table model main.fct_releases ............................... [RUN]
20:23:06 | 20 | prefect.task_runs | Finished in state Completed()
20:23:07 | 20 | prefect.task_runs | 20 of 32 OK created sql table model main.fct_releases .......................... [OK in 0.26s]
20:23:07 | 20 | prefect.task_runs | 21 of 32 START test accepted_values_fct_pull_requests_state__open__closed ...... [RUN]
20:23:07 | 20 | prefect.task_runs | Finished in state Completed()
20:23:07 | 20 | prefect.task_runs | 21 of 32 PASS accepted_values_fct_pull_requests_state__open__closed ............ [PASS in 0.15s]
20:23:07 | 20 | prefect.task_runs | 22 of 32 START test not_null_fct_pull_requests_pr_key .......................... [RUN]
20:23:07 | 20 | prefect.task_runs | 22 of 32 PASS not_null_fct_pull_requests_pr_key ................................ [PASS in 0.16s]
20:23:07 | 20 | prefect.task_runs | 23 of 32 START test unique_fct_pull_requests_pr_key ............................ [RUN]
20:23:07 | 20 | prefect.task_runs | Finished in state Completed()
20:23:07 | 20 | prefect.task_runs | Finished in state Completed()
20:23:07 | 20 | prefect.task_runs | 23 of 32 PASS unique_fct_pull_requests_pr_key .................................. [PASS in 0.19s]
20:23:07 | 20 | prefect.task_runs | 24 of 32 START test not_null_fct_releases_release_key .......................... [RUN]
20:23:07 | 20 | prefect.task_runs | 24 of 32 PASS not_null_fct_releases_release_key ................................ [PASS in 0.24s]
20:23:07 | 20 | prefect.task_runs | 25 of 32 START test unique_fct_releases_release_key ............................ [RUN]
20:23:08 | 20 | prefect.task_runs | Finished in state Completed()
20:23:08 | 20 | prefect.task_runs | 25 of 32 PASS unique_fct_releases_release_key .................................. [PASS in 0.31s]
20:23:08 | 20 | prefect.task_runs | 26 of 32 START sql table model main.mart_repo_monthly .......................... [RUN]
20:23:08 | 20 | prefect.task_runs | Finished in state Completed()
20:23:08 | 20 | prefect.task_runs | Finished in state Completed()
20:23:08 | 20 | prefect.task_runs | Finished in state Completed()
20:23:08 | 20 | prefect.task_runs | 26 of 32 OK created sql table model main.mart_repo_monthly ..................... [OK in 0.61s]
20:23:08 | 20 | prefect.task_runs | 27 of 32 START test not_null_mart_repo_monthly_month ........................... [RUN]
20:23:08 | 20 | prefect.task_runs | Finished in state Completed()
20:23:09 | 20 | prefect.task_runs | 27 of 32 PASS not_null_mart_repo_monthly_month ................................. [PASS in 0.21s]
20:23:09 | 20 | prefect.task_runs | 28 of 32 START test not_null_mart_repo_monthly_repo ............................ [RUN]
20:23:09 | 20 | prefect.task_runs | Finished in state Completed()
20:23:09 | 20 | prefect.task_runs | 28 of 32 PASS not_null_mart_repo_monthly_repo .................................. [PASS in 0.22s]
20:23:09 | 20 | prefect.task_runs | 29 of 32 START test unique_mart_repo_monthly_repo_month ........................ [RUN]
20:23:09 | 20 | prefect.task_runs | 29 of 32 PASS unique_mart_repo_monthly_repo_month .............................. [PASS in 0.17s]
20:23:09 | 20 | prefect.task_runs | Finished in state Completed()
20:23:09 | 20 | prefect.task_runs | Finished in state Completed()
20:23:09 | 20 | prefect.task_runs | 30 of 32 START sql table model main.mart_health_inputs ......................... [RUN]
20:23:09 | 20 | prefect.task_runs | 30 of 32 OK created sql table model main.mart_health_inputs .................... [OK in 0.27s]
20:23:09 | 20 | prefect.task_runs | Finished in state Completed()
20:23:09 | 20 | prefect.task_runs | 31 of 32 START test not_null_mart_health_inputs_repo ........................... [RUN]
20:23:10 | 20 | prefect.task_runs | 31 of 32 PASS not_null_mart_health_inputs_repo ................................. [PASS in 0.22s]
20:23:10 | 20 | prefect.task_runs | 32 of 32 START test unique_mart_health_inputs_repo ............................. [RUN]
20:23:10 | 20 | prefect.task_runs | Finished in state Completed()
20:23:10 | 20 | prefect.task_runs | 32 of 32 PASS unique_mart_health_inputs_repo ................................... [PASS in 0.21s]
20:23:10 | 20 | prefect.task_runs | Finished in state Completed()
20:23:10 | 20 | prefect.task_runs | Finished in state Completed()
20:23:10 | 20 | prefect.task_runs | Finished in state Completed()
20:23:10 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished running 4 table models, 24 data tests, 4 view models in 0 hours 0 minutes and 7.90 seconds (7.90s).
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:11 | 20 | prefect.task_runs | Completed successfully
20:23:11 | 20 | prefect.task_runs | Done. PASS=32 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=32
20:23:11 | 20 | prefect.task_runs | SUCCESS model stg_commits
20:23:11 | 20 | prefect.task_runs | SUCCESS model stg_pr_reviews
20:23:11 | 20 | prefect.task_runs | SUCCESS model stg_pull_requests
20:23:11 | 20 | prefect.task_runs | SUCCESS model stg_releases
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_stg_commits_commit_key
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_stg_commits_commit_key
20:23:11 | 20 | prefect.task_runs | PASS    test  accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_stg_pr_reviews_pr_key
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_stg_pr_reviews_review_key
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_stg_pr_reviews_review_key
20:23:11 | 20 | prefect.task_runs | PASS    test  accepted_values_stg_pull_requests_state__open__closed
20:23:11 | 20 | prefect.task_runs | PASS    test  assert_pr_closed_after_created
20:23:11 | 20 | prefect.task_runs | PASS    test  assert_review_after_pr_created
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_stg_pull_requests_pr_key
20:23:11 | 20 | prefect.task_runs | PASS    test  relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_stg_pull_requests_pr_key
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_stg_releases_release_key
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_stg_releases_release_key
20:23:11 | 20 | prefect.task_runs | SUCCESS model fct_pull_requests
20:23:11 | 20 | prefect.task_runs | SUCCESS model fct_releases
20:23:11 | 20 | prefect.task_runs | PASS    test  accepted_values_fct_pull_requests_state__open__closed
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_fct_pull_requests_pr_key
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_fct_pull_requests_pr_key
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_fct_releases_release_key
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_fct_releases_release_key
20:23:11 | 20 | prefect.task_runs | SUCCESS model mart_repo_monthly
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_mart_repo_monthly_month
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_mart_repo_monthly_repo
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_mart_repo_monthly_repo_month
20:23:11 | 20 | prefect.task_runs | SUCCESS model mart_health_inputs
20:23:11 | 20 | prefect.task_runs | PASS    test  not_null_mart_health_inputs_repo
20:23:11 | 20 | prefect.task_runs | PASS    test  unique_mart_health_inputs_repo
20:23:11 | 20 | prefect.task_runs | dbt build passed: 32 models and tests
20:23:11 | 20 | prefect.task_runs | Finished in state Completed()
20:23:12 | 20 | prefect.task_runs | networkx/networkx commits: 902 landed records, 0 missing from raw_commits
20:23:12 | 20 | prefect.task_runs | numpy/numpy commits: 7736 landed records, 0 missing from raw_commits
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels commits: 3365 landed records, 0 missing from raw_commits
20:23:12 | 20 | prefect.task_runs | networkx/networkx commits: 902 raw rows, 0 dropped on purpose, 902 in stg_commits
20:23:12 | 20 | prefect.task_runs | numpy/numpy commits: 7736 raw rows, 0 dropped on purpose, 7736 in stg_commits
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels commits: 3365 raw rows, 0 dropped on purpose, 3365 in stg_commits
20:23:12 | 20 | prefect.task_runs | networkx/networkx pulls: 1500 landed records, 0 missing from raw_pull_requests
20:23:12 | 20 | prefect.task_runs | numpy/numpy pulls: 5900 landed records, 0 missing from raw_pull_requests
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels pulls: 1200 landed records, 0 missing from raw_pull_requests
20:23:12 | 20 | prefect.task_runs | networkx/networkx pulls: 1500 raw rows, 0 dropped on purpose, 1500 in stg_pull_requests
20:23:12 | 20 | prefect.task_runs | numpy/numpy pulls: 5900 raw rows, 0 dropped on purpose, 5900 in stg_pull_requests
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels pulls: 1200 raw rows, 0 dropped on purpose, 1200 in stg_pull_requests
20:23:12 | 20 | prefect.task_runs | networkx/networkx pr_reviews: 5012 landed records, 0 missing from raw_pr_reviews
20:23:12 | 20 | prefect.task_runs | numpy/numpy pr_reviews: 12825 landed records, 0 missing from raw_pr_reviews
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels pr_reviews: 992 landed records, 0 missing from raw_pr_reviews
20:23:12 | 20 | prefect.task_runs | networkx/networkx pr_reviews: 5012 raw rows, 0 dropped on purpose, 5012 in stg_pr_reviews
20:23:12 | 20 | prefect.task_runs | numpy/numpy pr_reviews: 12825 raw rows, 0 dropped on purpose, 12825 in stg_pr_reviews
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels pr_reviews: 992 raw rows, 0 dropped on purpose, 992 in stg_pr_reviews
20:23:12 | 20 | prefect.task_runs | networkx/networkx releases: 67 landed records, 0 missing from raw_releases
20:23:12 | 20 | prefect.task_runs | numpy/numpy releases: 149 landed records, 0 missing from raw_releases
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels releases: 32 landed records, 0 missing from raw_releases
20:23:12 | 20 | prefect.task_runs | networkx/networkx releases: 67 raw rows, 0 dropped on purpose, 67 in stg_releases
20:23:12 | 20 | prefect.task_runs | numpy/numpy releases: 149 raw rows, 0 dropped on purpose, 149 in stg_releases
20:23:12 | 20 | prefect.task_runs | statsmodels/statsmodels releases: 32 raw rows, 0 dropped on purpose, 32 in stg_releases
20:23:12 | 20 | prefect.task_runs | Commits in every complete month: checked 34 months per repo, 0 empty
20:23:12 | 20 | prefect.task_runs | All completeness checks passed
20:23:12 | 20 | prefect.task_runs | Time-to-merge outliers (merged > 8760 h = 1 year after opening): 16 PRs, kept
                   repo  number  days_to_merge    created     merged
      networkx/networkx    7475          689.8 2024-06-01 2026-04-22
      networkx/networkx    7487          509.6 2024-06-05 2025-10-27
      networkx/networkx    7249          419.0 2024-01-23 2025-03-17
      networkx/networkx    7347          365.3 2024-03-13 2025-03-14
            numpy/numpy   26214          907.3 2024-04-04 2026-09-29
            numpy/numpy   26255          894.6 2024-04-10 2026-09-22
            numpy/numpy   26574          623.1 2024-05-30 2026-02-12
            numpy/numpy   28574          479.8 2025-03-23 2026-07-16
            numpy/numpy   27646          458.1 2024-10-26 2026-01-27
            numpy/numpy   28759          455.4 2025-04-18 2026-07-17
statsmodels/statsmodels    9297          786.6 2024-06-30 2026-08-26
statsmodels/statsmodels    9309          757.5 2024-07-13 2026-08-09
statsmodels/statsmodels    9477          572.3 2025-01-16 2026-08-11
statsmodels/statsmodels    9484          566.0 2025-01-20 2026-08-09
statsmodels/statsmodels    9510          535.6 2025-02-18 2026-08-08
statsmodels/statsmodels    9571          440.6 2025-05-26 2026-08-10
20:23:12 | 20 | prefect.task_runs | Weekly-commit outliers (above Q3 + 3 x IQR per repo): 12 weeks, kept
                   repo       week  commits  fence
      networkx/networkx 2024-07-01       29  23.00
            numpy/numpy 2025-10-13      166 151.75
statsmodels/statsmodels 2026-09-28      363  58.00
statsmodels/statsmodels 2026-10-26      285  58.00
statsmodels/statsmodels 2026-10-19      242  58.00
statsmodels/statsmodels 2026-08-03      236  58.00
statsmodels/statsmodels 2026-08-17      195  58.00
statsmodels/statsmodels 2026-08-10      158  58.00
statsmodels/statsmodels 2026-07-20      154  58.00
statsmodels/statsmodels 2026-08-24      152  58.00
statsmodels/statsmodels 2026-07-27      134  58.00
statsmodels/statsmodels 2026-10-05       91  58.00
20:23:12 | 20 | prefect.task_runs | Descriptive statistics (human PRs; hours)
                   repo  median_first_review_h  p90_first_review_h  median_merge_h  p90_merge_h  commits_per_week  releases_per_year
      networkx/networkx                   12.6               252.6            60.9       1128.2               5.9                4.6
            numpy/numpy                    7.5               170.1             7.8        276.8              49.4               14.8
statsmodels/statsmodels                   14.5               486.8             5.0        286.0              22.2                2.1
20:23:12 | 20 | prefect.task_runs | Finished in state Completed()
20:23:12 | 20 | prefect.task_runs | Inputs from mart_health_inputs
                         commits_per_week  commit_trend_pct  pct_reviewed_within_7d  median_hours_to_merge  pct_open_stale  releases_per_year  bus_factor
repo                                                                                                                                                     
networkx/networkx                  5.9354           -0.0031                  0.7199                60.9258          0.5914             4.5751           4
numpy/numpy                       49.4118           -0.0101                  0.3783                 7.7672          0.3142            14.7810           3
statsmodels/statsmodels           22.1834            0.1085                  0.1277                 5.0400          0.5319             2.1116           1
20:23:12 | 20 | prefect.task_runs | Health ranking (score 0-100; dimension subscores 0-1, 1 = healthiest of the three), written to health_scores
 rank                    repo  score  responsiveness  contributor_base  activity_trend  release_cadence  backlog  throughput
    1             numpy/numpy 63.427           0.423             0.667           0.000            1.000    1.000       0.951
    2       networkx/networkx 48.801           1.000             1.000           0.059            0.194    0.000       0.000
    3 statsmodels/statsmodels 28.219           0.000             0.000           1.000            0.000    0.215       1.000
20:23:12 | 20 | prefect.task_runs | Finished in state Completed()
20:23:15 | 20 | prefect.task_runs | Saved plots/dimension_profile.png
20:23:15 | 20 | prefect.task_runs | Saved plots/monthly_trends.png
20:23:15 | 20 | prefect.task_runs | Saved plots/review_vs_merge.png
20:23:15 | 20 | prefect.task_runs | Finished in state Completed()
20:23:16 | 20 | prefect.flow_runs | Finished in state Completed()
```
