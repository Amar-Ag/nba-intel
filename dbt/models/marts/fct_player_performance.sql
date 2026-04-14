with stg as (
    select * from {{ ref('stg_player_game_logs') }}
)

select
    *,
    avg(pts) over (
        partition by player_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_pts,

    avg(ast) over (
        partition by player_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_ast,

    avg(reb) over (
        partition by player_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_reb,

    round(pts / nullif(2 * (fga + 0.44 * fta), 0), 3) as true_shooting_pct,

    row_number() over (
        partition by player_id
        order by game_date
    ) as game_number

from stg