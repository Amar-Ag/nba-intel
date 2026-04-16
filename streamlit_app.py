import streamlit as st
from rag.chain import ask

st.set_page_config(
    page_title="NBA Intel",
    page_icon="🏀",
    layout="centered"
)

st.title("🏀 NBA Intel")
st.caption("Ask me anything about the current NBA season — powered by a local RAG pipeline")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input
if prompt := st.chat_input("Ask about NBA stats, standings, streaks..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get answer
    with st.chat_message("assistant"):
        with st.spinner("Searching game data..."):
            result = ask(prompt)
            st.markdown(result["answer"])
            with st.expander("View source data"):
                st.text(result["context"][:1000])

    # Add assistant message to history
    st.session_state.messages.append({
        "role": "assistant",
        "content": result["answer"]
    })