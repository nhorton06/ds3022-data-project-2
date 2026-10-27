-- Fails (returns rows) if any PR was closed or merged before it was created.
select pr_key, created_at, closed_at, merged_at
from {{ ref('stg_pull_requests') }}
where closed_at < created_at
   or merged_at < created_at
