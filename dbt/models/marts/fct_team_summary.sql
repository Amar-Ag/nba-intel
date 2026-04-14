with stg as (
    select * from {{ ref('stg_team_game_logs') }}
)

select
    *,
    avg(pts) over (
        partition by team_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_pts,

    avg(ast) over (
        partition by team_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_ast,

    avg(reb) over (
        partition by team_id
        order by game_date
        rows between 9 preceding and current row
    ) as rolling_10g_reb,

    sum(case when win_loss = 'W' then 1 else 0 end) over (
        partition by team_id
        order by game_date
        rows between 9 preceding and current row
    ) as wins_last_10,

    row_number() over (
        partition by team_id
        order by game_date
    ) as game_number

from stg