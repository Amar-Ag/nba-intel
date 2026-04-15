# ingestion/minio_to_duckdb.py

import boto3
import duckdb
import pandas as pd
from io import BytesIO

# 1. Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://172.18.0.4:9000', 
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# 2. Connect to DuckDB
con = duckdb.connect("/workspaces/nba-intel/airflow/nba.duckdb")

# 3. Function to load a parquet from MinIO into DuckDB
def load_table(bucket: str, prefix: str, table_name: str):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print(f"No objects found with prefix {prefix} in bucket {bucket}")
        return

    # b. Read each parquet file into a dataframe
    frames = []
    for obj in response['Contents']:
        key = obj['Key']
        print(f"Reading {key}...")
        obj_response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(obj_response['Body'].read()))
        frames.append(df)

    # c. Combine all dataframes
    combined = pd.concat(frames).drop_duplicates()

    # d. Load into DuckDB
    con.execute(f"CREATE OR REPLACE TABLE raw_{table_name} AS SELECT * FROM combined")
    print(f"Loaded {len(combined)} rows into raw_{table_name}")

# 4. Main — load all three tables
if __name__ == "__main__":
    load_table("nba-raw", "player_game_logs/", "player_game_logs")
    load_table("nba-raw", "team_game_logs/", "team_game_logs")
    load_table("nba-raw", "standings/", "standings")