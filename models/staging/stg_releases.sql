-- One row per published release. Drafts are dropped: they are unpublished and have no published_at.
select
    repo,
    id,
    repo || '#' || id as release_key,
    payload->>'tag_name' as tag_name,
    (payload->>'published_at')::TIMESTAMPTZ as published_at,
    (payload->>'prerelease')::BOOLEAN as prerelease,
    fetched_at::TIMESTAMPTZ as fetched_at
from {{ source('raw', 'raw_releases') }}
where not (payload->>'draft')::BOOLEAN
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
