-- One row per repo per month, from the window start through the run month. Months with no activity show 0.
-- Bot-authored PRs and commits are excluded: they are automated and say nothing about the human community.
with months as (
    select generate_series::DATE as month
    from generate_series(
        TIMESTAMP '{{ var("window_start") }}',
        date_trunc('month', {{ run_time() }})::TIMESTAMP,
        interval 1 month
    )
),

repos as (
    select distinct repo from {{ ref('stg_commits') }}
),

-- Every repo x every month, so empty months still get a row
spine as (
    select r.repo, m.month
    from repos r
    cross join months m
),

human_prs as (
    select * from {{ ref('fct_pull_requests') }}
    where not is_bot
),

-- Each event is counted in the month it happened
opened as (
    select
        repo,
        date_trunc('month', created_at)::DATE as month,
        count(*) as prs_opened,
        median(hours_to_first_review) as median_first_review_hours,
        count(distinct author_login) as active_contributors
    from human_prs
    group by all
),

merged as (
    select
        repo,
        date_trunc('month', merged_at)::DATE as month,
        count(*) as prs_merged,
        median(hours_to_merge) as median_merge_hours
    from human_prs
    where is_merged
    group by all
),

closed_unmerged as (
    select
        repo,
        date_trunc('month', closed_at)::DATE as month,
        count(*) as prs_closed_unmerged
    from human_prs
    where is_closed_unmerged
    group by all
),

commits as (
    select
        repo,
        date_trunc('month', committed_at)::DATE as month,
        count(*) as commits
    from {{ ref('stg_commits') }}
    where not is_bot
      and committed_at >= '{{ var("window_start") }}'
    group by all
)

select
    s.repo,
    s.month,
    date_diff('month', DATE '{{ var("window_start") }}', s.month) as month_index,
    coalesce(o.prs_opened, 0) as prs_opened,
    coalesce(m.prs_merged, 0) as prs_merged,
    coalesce(cu.prs_closed_unmerged, 0) as prs_closed_unmerged,
    coalesce(m.prs_merged, 0)
        / nullif(coalesce(m.prs_merged, 0) + coalesce(cu.prs_closed_unmerged, 0), 0) as merge_ratio,
    o.median_first_review_hours,
    m.median_merge_hours,
    coalesce(c.commits, 0) as commits,
    coalesce(o.active_contributors, 0) as active_contributors,
    s.month = date_trunc('month', {{ run_time() }})::DATE as is_partial_month
from spine s
left join opened o using (repo, month)
left join merged m using (repo, month)
left join closed_unmerged cu using (repo, month)
left join commits c using (repo, month)
