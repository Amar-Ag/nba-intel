with source as (
    select * from {{ source('raw', 'raw_player_game_logs') }}
),

renamed as (
    select
        season_year,
        player_id,
        player_name,
        team_id,
        team_abbreviation               as team_abbr,
        game_id,
        cast(game_date as date)         as game_date,
        matchup,
        wl                              as win_loss,
        min                             as minutes,
        fgm, fga, fg_pct,
        fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct,
        oreb, dreb, reb,
        ast, tov, stl, blk, pf,
        pts,
        plus_minus
    from source
)

select * from renamed