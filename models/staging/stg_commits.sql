-- One row per commit. author_login is the linked GitHub account; NULL when the commit email isn't linked to one.
select
    repo,
    id as sha,
    repo || '#' || id as commit_key,
    payload->'author'->>'login' as author_login,
    {{ is_bot("payload->'author'") }} as is_bot,
    (payload->'commit'->'author'->>'date')::TIMESTAMPTZ as committed_at,
    fetched_at::TIMESTAMPTZ as fetched_at
from {{ source('raw', 'raw_commits') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
