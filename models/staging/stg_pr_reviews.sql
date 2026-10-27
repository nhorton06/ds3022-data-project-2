-- One submitted review per row. PENDING reviews are dropped: they have no submitted_at.
with reviews as (
    select
        *,
        -- pull_request_url ends in the PR number, e.g. .../repos/numpy/numpy/pulls/12345
        regexp_extract(payload->>'pull_request_url', '(\d+)$', 1)::BIGINT as pr_number
    from {{ source('raw', 'raw_pr_reviews') }}
    where (payload->>'state') <> 'PENDING'
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id,
    repo || '#' || id as review_key,
    pr_number,
    repo || '#' || pr_number as pr_key,
    payload->'user'->>'login' as reviewer_login,
    {{ is_bot("payload->'user'") }} as is_bot,
    payload->>'state' as state,
    (payload->>'submitted_at')::TIMESTAMPTZ as submitted_at,
    fetched_at::TIMESTAMPTZ as fetched_at
from reviews
