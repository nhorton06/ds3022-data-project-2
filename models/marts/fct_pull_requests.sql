-- One row per PR created on or after the window start, with review timing, outcome flags, and durations.
-- Open PRs are kept: their merge/close columns stay NULL rather than getting a fake end date.
with prs as (
    select * from {{ ref('stg_pull_requests') }}
    where created_at >= '{{ var("window_start") }}'
),

-- Reviews that count toward responsiveness: not by a bot and not by the PR's own author
counted_reviews as (
    select r.pr_key, r.state, r.submitted_at
    from {{ ref('stg_pr_reviews') }} r
    join prs p on p.pr_key = r.pr_key
    where not r.is_bot
      and r.reviewer_login is distinct from p.author_login
),

review_summary as (
    select
        pr_key,
        min(submitted_at) as first_review_at,
        bool_or(state = 'APPROVED') as has_approving_review
    from counted_reviews
    group by pr_key
)

select
    p.repo,
    p.number,
    p.pr_key,
    p.author_login,
    p.is_bot,
    p.state,
    p.created_at,
    p.updated_at,
    p.closed_at,
    p.merged_at,
    p.merged_at is not null as is_merged,
    p.state = 'closed' and p.merged_at is null as is_closed_unmerged,
    p.state = 'open' as is_open,
    r.first_review_at,
    date_diff('second', p.created_at, r.first_review_at) / 3600.0 as hours_to_first_review,
    coalesce(r.first_review_at <= p.created_at + interval 7 day, false) as reviewed_within_7d,
    date_diff('second', p.created_at, p.merged_at) / 3600.0 as hours_to_merge,
    case when p.state = 'closed' then date_diff('second', p.created_at, p.closed_at) / 3600.0 end as hours_to_close,
    p.state = 'open' and p.updated_at < {{ run_time() }} - interval 180 day as is_stale,
    coalesce(r.has_approving_review, false) as has_approving_review
from prs p
left join review_summary r on r.pr_key = p.pr_key
