{#
    "Run time" = the newest fetched_at in the raw tables (not now()), so rebuilding
    without new data gives the same results. Used for is_stale and the partial month.
#}
{% macro run_time() %}
    (
        select max(fetched_at)::TIMESTAMPTZ
        from (
            select fetched_at from {{ source('raw', 'raw_commits') }}
            union all select fetched_at from {{ source('raw', 'raw_pull_requests') }}
            union all select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
            union all select fetched_at from {{ source('raw', 'raw_releases') }}
        )
    )
{% endmacro %}
