# rag/embed.py

import duckdb
import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction

# 1. Connect to DuckDB
con = duckdb.connect("/workspaces/nba-intel/nba.duckdb")

# 2. Connect to ChromaDB
client = chromadb.HttpClient(host='172.18.0.3', port=8000)

# 3. Set up Ollama embeddings
embedding_function = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="nomic-embed-text"
)

collection = client.get_or_create_collection(
    name="player_performance",
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

# 6. Main
if __name__ == "__main__":
    embed_player_performance()