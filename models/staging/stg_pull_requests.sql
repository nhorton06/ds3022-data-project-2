-- One row per pull request, open or closed. closed_at and merged_at stay NULL until those events happen.
select
    repo,
    id as number,
    repo || '#' || id as pr_key,
    payload->'user'->>'login' as author_login,
    {{ is_bot("payload->'user'") }} as is_bot,
    payload->>'state' as state,
    (payload->>'created_at')::TIMESTAMPTZ as created_at,
    (payload->>'updated_at')::TIMESTAMPTZ as updated_at,
    (payload->>'closed_at')::TIMESTAMPTZ as closed_at,
    (payload->>'merged_at')::TIMESTAMPTZ as merged_at,
    fetched_at::TIMESTAMPTZ as fetched_at
from {{ source('raw', 'raw_pull_requests') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
