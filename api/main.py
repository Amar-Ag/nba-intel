# api/main.py
from fastapi import FastAPI
from pydantic import BaseModel
from rag.chain import ask

app = FastAPI(title="NBA Intel API")

class Question(BaseModel):
    question: str

@app.post("/ask")
async def ask_question(body: Question):
    result = ask(body.question)
    return result

@app.get("/health")
async def health():
    return {"status": "healthy"}