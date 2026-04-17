from chain import ask
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy
from datasets import Dataset
import mlflow
import socket

# Fix MLflow to point to our container
mlflow_host = socket.gethostbyname('nba_mlflow')
mlflow.set_tracking_uri(f"http://{mlflow_host}:5000")
mlflow.set_experiment("nba_intel_rag_evaluation")

eval_questions = [
    # Standings
    "Which team is leading the Eastern conference?",
    "Which team has the worst record in the NBA?",
    "Which team has the best point differential?",
    "What is the Pistons current record?",
    # Trends
    "Which team has the most wins in the last 10 games?",
    "Who has the longest current winning streak?",
    "Which team started hot but fizzled out?",
    # Individual players
    "Who has the best true shooting percentage this season?",
    "Which player has the highest rolling 10-game scoring average?",
    "Who has been averaging the most assists recently?",
    "Who has been the most consistent rebounder this season?",
    "Is there an underrated player whose contributions are not as loud?",
    # Team stats
    "Which team has the best three point shooting percentage?",
    "Which team scores the most points per game?",
    "Which team has the best home record?",
    # Comparisons
    "How does OKC compare to the Spurs this season?",
    "Which conference is stronger this season?",
    "Who is more efficient, LeBron James or Nikola Jokic?",
    # Complex reasoning
    "Which team is most likely to make a deep playoff run?",
    "Which player has improved the most over the last 10 games?",
]

from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_ollama import ChatOllama, OllamaEmbeddings

# Configure RAGAS to use local Ollama instead of OpenAI
ollama_llm = LangchainLLMWrapper(ChatOllama(model="llama3.2:1b"))
ollama_embeddings = LangchainEmbeddingsWrapper(OllamaEmbeddings(model="nomic-embed-text"))

# Apply to metrics
faithfulness.llm = ollama_llm
answer_relevancy.llm = ollama_llm
answer_relevancy.embeddings = ollama_embeddings

def run_evaluation():
    print("Running evaluation on all questions...")
    
    results = []
    for i, question in enumerate(eval_questions):
        print(f"[{i+1}/{len(eval_questions)}] {question}")
        result = ask(question)
        results.append({
            "question": question,
            "answer": result["answer"],
            "contexts": [result["context"]],
        })
    
    # Build RAGAS dataset
    dataset = Dataset.from_list(results)
    
    # Score with RAGAS
    print("\nScoring with RAGAS...")
    scores = evaluate(
        dataset,
        metrics=[faithfulness, answer_relevancy],
    )
    
    print(f"\nResults:")
    print(f"Faithfulness: {scores['faithfulness']:.3f}")
    print(f"Answer Relevancy: {scores['answer_relevancy']:.3f}")
    
    # Log to MLflow
    with mlflow.start_run(run_name="rag_evaluation"):
        mlflow.log_param("model", "llama3.2:1b")
        mlflow.log_param("embedding_model", "nomic-embed-text")
        mlflow.log_param("num_questions", len(eval_questions))
        mlflow.log_metric("faithfulness", scores["faithfulness"])
        mlflow.log_metric("answer_relevancy", scores["answer_relevancy"])
        
        # Log each question/answer as artifact
        with open("/tmp/eval_results.txt", "w") as f:
            for r in results:
                f.write(f"Q: {r['question']}\n")
                f.write(f"A: {r['answer']}\n")
                f.write("-" * 50 + "\n")
        mlflow.log_artifact("/tmp/eval_results.txt")
    
    print("\nResults logged to MLflow at http://localhost:5000")
    return scores

if __name__ == "__main__":
    run_evaluation()