-- One row per repo, one column per health metric scored in Task 5 (one metric per dimension), plus
-- commits_per_week, which Task 4 reports but Task 5 does not score (it rewards size, not health).
-- Every column is a rate, share, median, slope, or headcount, never a raw volume total.
-- Bot-authored PRs and commits are excluded (see mart_repo_monthly).
with window_info as (
    select
        {{ run_time() }} as run_time,
        date_diff('second', TIMESTAMPTZ '{{ var("window_start") }}', {{ run_time() }}) / 86400.0 as days_in_window
),

repos as (
    select distinct repo from {{ ref('stg_commits') }}
),

human_prs as (
    select * from {{ ref('fct_pull_requests') }}
    where not is_bot
),

pr_metrics as (
    select
        repo,
        -- only PRs old enough to have had a full 7 days to be reviewed
        avg(reviewed_within_7d::INT)
            filter (where created_at <= (select run_time from window_info) - interval 7 day) as pct_reviewed_within_7d,
        median(hours_to_merge) as median_hours_to_merge,
        avg(is_stale::INT) filter (where is_open) as pct_open_stale
    from human_prs
    group by repo
),

commit_metrics as (
    select
        repo,
        sum(commits) / ((select days_in_window from window_info) / 7) as commits_per_week,
        -- monthly slope divided by the average month = relative change per month, comparable across repo sizes
        regr_slope(commits, month_index) filter (where not is_partial_month)
            / avg(commits) filter (where not is_partial_month) as commit_trend_pct
    from {{ ref('mart_repo_monthly') }}
    group by repo
),

release_metrics as (
    select
        repo,
        count(*) / ((select days_in_window from window_info) / 365.25) as releases_per_year
    from {{ ref('fct_releases') }}
    group by repo
),

-- Bus factor: the fewest authors who together account for at least half of the merged PRs
author_merges as (
    select repo, author_login, count(*) as merged_prs
    from human_prs
    where is_merged
    group by all
),

ranked_authors as (
    select
        repo,
        row_number() over (partition by repo order by merged_prs desc, author_login) as author_rank,
        sum(merged_prs) over (
            partition by repo order by merged_prs desc, author_login rows unbounded preceding
        ) as running_merged_prs,
        sum(merged_prs) over (partition by repo) as total_merged_prs
    from author_merges
),

bus_factor as (
    select repo, min(author_rank) as bus_factor
    from ranked_authors
    where running_merged_prs >= 0.5 * total_merged_prs
    group by repo
)

select
    r.repo,
    c.commits_per_week,
    c.commit_trend_pct,
    p.pct_reviewed_within_7d,
    p.median_hours_to_merge,
    p.pct_open_stale,
    coalesce(rel.releases_per_year, 0) as releases_per_year,
    b.bus_factor
from repos r
left join pr_metrics p using (repo)
left join commit_metrics c using (repo)
left join release_metrics rel using (repo)
left join bus_factor b using (repo)
