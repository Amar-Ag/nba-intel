# imports
import chromadb
from chromadb.utils.embedding_functions.ollama_embedding_function import OllamaEmbeddingFunction
import socket

import ollama

# 1. Connect to Chromadb and resolve container names dynamically
chroma_host = socket.gethostbyname('nba_chromadb')
client = chromadb.HttpClient(host=chroma_host, port=8000)

# 2. Set up Ollama embeddings
embedding_function = OllamaEmbeddingFunction(
    url="http://localhost:11434",
    model_name="nomic-embed-text"
)

# 3. Get or create the collection
collection = client.get_or_create_collection(
    name="player_performance",
    embedding_function=embedding_function
)

# 4. Function to retrieve relevant summaries
def retrieve(question: str, k: int = 8) -> str:
    question_embedding = embedding_function([question])
    results = collection.query(
        query_embeddings=question_embedding,
        n_results=k
    )
    docs = results["documents"][0] if results["documents"] else []
    return "\n\n".join(docs)

# 5. Function to ask a question
def ask(question: str) -> dict:
    context = retrieve(question)
    
    prompt = f"""You are an NBA analyst. Answer the question using ONLY the game data provided below.
    If the data doesn't contain enough information, say so clearly. Do not make up statistics.

    Game data:
    {context}

    Question: {question}

    Answer:"""
    
    response = ollama.chat(
        model='llama3.2',
        messages=[{'role': 'user', 'content': prompt}]
    )
    
    return {
        "answer": response['message']['content'],
        "context": context
    }

# 6. Main — test with a sample question
if __name__ == "__main__":
    question = "Who has been the best scorer in the last 10 games?"
    result = ask(question)
    print("Answer:", result["answer"])
    print("\nSources used:")
    print(result["context"][:500])