with stg as (
    select * from {{ ref('stg_standings') }}
)

select
    *,
    wins + losses as total_games
from stg