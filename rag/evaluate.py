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


def llm_judge(question: str, answer: str, context: str) -> dict:
    """Use llama3.2 to judge if the answer is faithful to the context."""
    
    prompt = f"""You are evaluating an NBA analyst AI system. 
    
Question: {question}

Context provided to the AI:
{context[:500]}

Answer given by the AI:
{answer}

Rate the answer on two criteria (score 0-1):
1. Faithfulness: Is the answer based only on the provided context? (1=fully faithful, 0=hallucinated)
2. Relevancy: Does the answer actually address the question? (1=fully relevant, 0=irrelevant)

Respond in this exact format:
FAITHFULNESS: 0.X
RELEVANCY: 0.X
REASON: one sentence explanation"""

    response = ollama.chat(
        model='llama3.2',
        messages=[{'role': 'user', 'content': prompt}]
    )
    
    text = response['message']['content']
    
    # Parse scores
    import re
    faith = float(re.search(r'FAITHFULNESS:\s*([\d.]+)', text).group(1))
    relev = float(re.search(r'RELEVANCY:\s*([\d.]+)', text).group(1))
    reason = re.search(r'REASON:\s*(.+)', text).group(1)
    
    return {"faithfulness": faith, "relevancy": relev, "reason": reason}

def run_evaluation():
    print("Running evaluation...")
    
    results = []
    faith_scores = []
    relev_scores = []
    
    for i, question in enumerate(eval_questions):
        print(f"[{i+1}/{len(eval_questions)}] {question}")
        result = ask(question)
        judgment = llm_judge(question, result["answer"], result["context"])
        
        faith_scores.append(judgment["faithfulness"])
        relev_scores.append(judgment["relevancy"])
        
        results.append({
            "question": question,
            "answer": result["answer"],
            "faithfulness": judgment["faithfulness"],
            "relevancy": judgment["relevancy"],
            "reason": judgment["reason"]
        })
        print(f"  Faith: {judgment['faithfulness']} | Relev: {judgment['relevancy']} | {judgment['reason']}")
    
    avg_faith = sum(faith_scores) / len(faith_scores)
    avg_relev = sum(relev_scores) / len(relev_scores)
    
    print(f"\nAverage Faithfulness: {avg_faith:.3f}")
    print(f"Average Relevancy: {avg_relev:.3f}")
    
    # Log to MLflow
    with mlflow.start_run(run_name="rag_evaluation"):
        mlflow.log_param("model", "llama3.2:1b")
        mlflow.log_param("judge_model", "llama3.2")
        mlflow.log_param("embedding_model", "nomic-embed-text")
        mlflow.log_param("num_questions", len(eval_questions))
        mlflow.log_metric("avg_faithfulness", avg_faith)
        mlflow.log_metric("avg_relevancy", avg_relev)
        
        # Save detailed results
        with open("/tmp/eval_results.txt", "w") as f:
            for r in results:
                f.write(f"Q: {r['question']}\n")
                f.write(f"A: {r['answer']}\n")
                f.write(f"Faith: {r['faithfulness']} | Relev: {r['relevancy']}\n")
                f.write(f"Reason: {r['reason']}\n")
                f.write("-" * 50 + "\n")
        mlflow.log_artifact("/tmp/eval_results.txt")
    
    print("Results logged to MLflow!")
    return avg_faith, avg_relev

if __name__ == "__main__":
    run_evaluation()