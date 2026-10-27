-- One row per published release on or after the window start.
-- lag() runs over ALL releases before the window filter, so the first release in the window
-- still measures its gap to the release before it.
with releases as (
    select
        *,
        lag(published_at) over (partition by repo order by published_at) as previous_published_at
    from {{ ref('stg_releases') }}
)

select
    repo,
    id,
    release_key,
    tag_name,
    published_at,
    prerelease as is_prerelease,
    date_diff('day', previous_published_at, published_at) as days_since_previous
from releases
where published_at >= '{{ var("window_start") }}'
