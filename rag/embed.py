# rag/embed.py

import duckdb
import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction

# 1. Connect to DuckDB
con = duckdb.connect("/opt/duckdb/nba.duckdb")

# 2. Connect to ChromaDB
import socket

# Resolve container names dynamically
chroma_host = socket.gethostbyname('nba_chromadb')
client = chromadb.HttpClient(host=chroma_host, port=8000)

# 3. Set up Ollama embeddings
embedding_function = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="nomic-embed-text"
)

collection = client.get_or_create_collection(
    name="player_performance",
    embedding_function=embedding_function
)

team_collection = client.get_or_create_collection(
    name="team_summary",
    embedding_function=embedding_function
)

standing_collection = client.get_or_create_collection(
    name="standings",
    embedding_function=embedding_function
)

# 4. Generate natural language summary from a row
def generate_player_summary(row) -> str:
    result = "won" if row["win_loss"] == "W" else "lost"
    return (
        f"{row['PLAYER_NAME']} scored {row['PTS']} points with "
        f"{row['AST']} assists and {row['REB']} rebounds "
        f"for {row['team_abbr']} against {row['MATCHUP']} "
        f"on {row['game_date'].strftime('%Y-%m-%d')}. "
        f"The {row['team_abbr']} {result}. "
        f"Shot {row['FG_PCT']:.1%} from the field. "
        f"Rolling 10-game averages: "
        f"{row['rolling_10g_pts']:.1f} pts, "
        f"{row['rolling_10g_ast']:.1f} ast, "
        f"{row['rolling_10g_reb']:.1f} reb. "
        f"True shooting %: {row['true_shooting_pct']:.1%}."
    )

def generate_team_summary(row) -> str:
    result = "won" if row["win_loss"] == "W" else "lost"
    opp_score = row["PTS"] - row["PLUS_MINUS"]
    return (
        f"{row['TEAM_NAME']} {result} against {row['MATCHUP']} "
        f"on {row['game_date'].strftime('%Y-%m-%d')}. "
        f"Score: {row['PTS']} - {int(opp_score)}. "
        f"Shot {row['FG_PCT']:.1%} from the field, {row['FG3_PCT']:.1%} from three. "
        f"Rolling 10-game averages: {row['rolling_10g_pts']:.1f} pts, "
        f"{row['rolling_10g_ast']:.1f} ast, {row['rolling_10g_reb']:.1f} reb. "
        f"Wins in last 10 games: {int(row['wins_last_10'])}."
    )

def generate_standings_summary(row) -> str:
    return (
        f"{row['team_name']} ({row['team_city']}) are {row['wins']}-{row['losses']} "
        f"with a {row['win_pct']:.1%} win percentage in the {row['conference']} conference. "
        f"Conference record: {row['conference_record']}. "
        f"Division rank: {row['division_rank']} in the {row['division']}. "
        f"Last 10 games: {row['l10']}. "
        f"Current streak: {row['current_streak']} games. "
        f"Points per game: {row['points_pg']:.1f}, "
        f"Opponent points per game: {row['opp_points_pg']:.1f}."
    )

# 5. Embed and store player summaries
def embed_player_performance():
    print("Reading from DuckDB...")
    df = con.execute("SELECT * FROM fct_player_performance").df()
    print(f"Found {len(df)} rows")

    summaries = []
    ids = []

    for _, row in df.iterrows():
        summary = generate_player_summary(row)
        unique_id = f"{row['PLAYER_ID']}_{row['GAME_ID']}"
        summaries.append(summary)
        ids.append(unique_id)

    # Add to ChromaDB in batches
    batch_size = 100
    total_batches = len(summaries) // batch_size + 1

    for i in range(0, len(summaries), batch_size):
        collection.upsert(
            documents=summaries[i:i+batch_size],
            ids=ids[i:i+batch_size]
        )
        print(f"Embedded batch {i//batch_size + 1} of {total_batches}")

    print(f"Done! Total embedded: {len(summaries)} summaries")

def embed_team_summary():
    print("Reading from DuckDB...")
    df = con.execute("SELECT * FROM fct_team_summary").df()
    print(f"Found {len(df)} rows")

    summaries = []
    ids = []

    for _, row in df.iterrows():
        summary = generate_team_summary(row)
        unique_id = f"{row['TEAM_ID']}_{row['GAME_ID']}"
        summaries.append(summary)
        ids.append(unique_id)

    # Add to ChromaDB in batches
    batch_size = 100
    total_batches = len(summaries) // batch_size + 1

    for i in range(0, len(summaries), batch_size):
        team_collection.upsert(
            documents=summaries[i:i+batch_size],
            ids=ids[i:i+batch_size]
        )
        print(f"Embedded batch {i//batch_size + 1} of {total_batches}")

    print(f"Done! Total embedded: {len(summaries)} summaries")

def embed_standings():
    print("Reading from DuckDB...")
    df = con.execute("SELECT * FROM fct_standings").df()
    print(f"Found {len(df)} rows")

    summaries = []
    ids = []

    for _, row in df.iterrows():
        summary = generate_standings_summary(row)
        unique_id = f"{row['team_id']}"
        summaries.append(summary)
        ids.append(unique_id)

    # Add to ChromaDB in batches
    batch_size = 100
    total_batches = len(summaries) // batch_size + 1

    for i in range(0, len(summaries), batch_size):
        standing_collection.upsert(
            documents=summaries[i:i+batch_size],
            ids=ids[i:i+batch_size]
        )
        print(f"Embedded batch {i//batch_size + 1} of {total_batches}")

    print(f"Done! Total embedded: {len(summaries)} summaries")



# 6. Main
if __name__ == "__main__":
    # embed_player_performance()
    embed_team_summary()
    embed_standings()