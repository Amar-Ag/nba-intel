# ingestion/nba_ingest.py

import socket

import boto3
import boto3.session
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
from nba_api.stats.endpoints import playergamelogs, teamgamelogs, leaguestandings

# 1. Connect to MinIO
minio_host = socket.gethostbyname('nba_minio')
s3 = boto3.client(
    's3',
    endpoint_url=f'http://{minio_host}:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1',
)

BUCKET = "nba-raw"
SEASON = "2024-25"

# 2. Upload dataframe as Parquet to MinIO
def upload_parquet(df: pd.DataFrame, key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"Uploaded {key} — {len(df)} rows")

# 3. Fetch player game logs
def ingest_player_logs():
    print("Fetching player game logs...")
    df = playergamelogs.PlayerGameLogs(
        season_nullable=SEASON
    ).get_data_frames()[0]
    date_str = datetime.now().strftime("%Y-%m-%d")
    upload_parquet(df, f"player_game_logs/season={SEASON}/date={date_str}/data.parquet")

# 4. Fetch team game logs
def ingest_team_logs():
    print("Fetching team game logs...")
    df = teamgamelogs.TeamGameLogs(
        season_nullable=SEASON
    ).get_data_frames()[0]
    date_str = datetime.now().strftime("%Y-%m-%d")
    upload_parquet(df, f"team_game_logs/season={SEASON}/date={date_str}/data.parquet")

# 5. Fetch standings
def ingest_standings():
    print("Fetching standings...")
    df = leaguestandings.LeagueStandings().get_data_frames()[0]
    date_str = datetime.now().strftime("%Y-%m-%d")
    upload_parquet(df, f"standings/date={date_str}/data.parquet")

# 6. Main
if __name__ == "__main__":
    # Create bucket if it doesn't exist
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")
    except Exception:
        print(f"Bucket {BUCKET} already exists")

    ingest_player_logs()
    ingest_team_logs()
    ingest_standings()
    print("Ingestion complete!")