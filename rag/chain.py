# imports
import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction
import socket

import ollama
import os
from groq import Groq
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('/workspaces/nba-intel/.env')  # Absolute path

load_dotenv(dotenv_path=env_path)
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))


# 1. Connect to Chromadb and resolve container names dynamically
chroma_host = socket.gethostbyname('nba_chromadb')
client = chromadb.HttpClient(host=chroma_host, port=8000)

# 2. Set up Ollama embeddings
embedding_function = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="nomic-embed-text"
)

# 3. Get or create the collection
player_collection = client.get_or_create_collection(
    name="player_performance",
    embedding_function=embedding_function
)
team_collection = client.get_or_create_collection(
    name="team_summary", 
    embedding_function=embedding_function
)
standings_collection = client.get_or_create_collection(
    name="standings",
    embedding_function=embedding_function
)

# 4. Function to retrieve relevant summaries
def retrieve(question: str, k: int = 5) -> str:
    question_embedding = embedding_function([question])
    
    # Always get standings context for conference/record questions
    standings_k = 10 if any(word in question.lower() for word in 
        ['conference', 'standing', 'record', 'best team', 'worst team', 'streak', 'leading']) else 3
    
    player_results = player_collection.query(
        query_embeddings=question_embedding,
        n_results=k
    )
    team_results = team_collection.query(
        query_embeddings=question_embedding,
        n_results=k
    )
    standings_results = standings_collection.query(
        query_embeddings=question_embedding,
        n_results=standings_k
    )
    
    # Put standings first so LLM sees it first
    all_docs = (
        standings_results["documents"][0] +
        team_results["documents"][0] +
        player_results["documents"][0]
    )
    
    return "\n\n".join(all_docs)

# 5. Function to ask a question
def ask(question: str) -> dict:
    context = retrieve(question)
    context_truncated = context[:1500]  # limit context length
    
    prompt = f"""You are an NBA analyst. Answer the question using ONLY the game data provided below.
Do NOT use any outside knowledge. If a team is mentioned in the data, use exactly what the data says.
If the data doesn't contain enough information, say "I don't have enough data to answer this."


    Game data:
    {context_truncated}

    Question: {question}

    Answer (based only on the data above):"""
    
    # response = ollama.chat(
    #     # model='llama3.2',
    #     # model='llama3.2:1b',  # changed from llama3.2
    #     model='mistral',
    #     # model='phi3:mini',
    #     messages=[{'role': 'user', 'content': prompt}]
    # )

    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    
    return {
        "answer": response.choices[0].message.content,  # Changed this line
        "context": context
    }
    # return {
    #     "answer": response['message']['content'],
    #     "context": context
    # }

# 6. Main — test with a sample question
if __name__ == "__main__":
    question = "Who has been the best scorer in the last 10 games?"
    result = ask(question)
    print("Answer:", result["answer"])
    print("\nSources used:")
    print(result["context"][:500])