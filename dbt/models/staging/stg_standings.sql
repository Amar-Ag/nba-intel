with source as (
    select * from {{ source('raw', 'raw_standings') }}
),
renamed as (
    select
        LeagueID                    as league_id,
        SeasonID                    as season_id,
        TeamID                      as team_id,
        TeamCity                    as team_city,
        TeamName                    as team_name,
        Conference                  as conference,
        ConferenceRecord            as conference_record,
        PlayoffRank                 as playoff_rank,
        ClinchIndicator             as clinch_indicator,
        Division                    as division,
        DivisionRecord              as division_record,
        DivisionRank                as division_rank,
        WINS                        as wins,
        LOSSES                      as losses,
        WinPCT                      as win_pct,
        LeagueRank                  as league_rank,
        Record                      as record,
        HOME                        as home,
        ROAD                        as road,
        OT                          as ot,
        L10                         as l10,
        LongWinStreak               as long_win_streak,
        LongLossStreak              as long_loss_streak,
        CurrentStreak               as current_streak,
        PointsPG                    as points_pg,
        OppPointsPG                 as opp_points_pg,
        DiffPointsPG                as diff_points_pg,
        ClinchedConferenceTitle     as clinched_conference_title,
        ClinchedDivisionTitle       as clinched_division_title,
        ClinchedPlayoffBirth        as clinched_playoff_birth
    from source
)

select * from renamed