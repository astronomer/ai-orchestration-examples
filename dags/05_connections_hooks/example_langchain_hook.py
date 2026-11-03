"""
## example_langchain_hook

`LangChainHook` builds LangChain chat and embedding models from a `langchain`
connection. The connection's password is the API key; when it's empty, as in
this project's `langchain_default`, the OpenAI client reads `OPENAI_API_KEY`
from the environment.
"""

from __future__ import annotations

from airflow.providers.common.ai.hooks.langchain import LangChainHook
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "LangChainHook", "langchain"],
    doc_md=__doc__,
)
def example_langchain_hook():
    @task
    def chat_and_embed() -> dict:
        hook = LangChainHook(
            llm_conn_id="langchain_default",
            llm_model="openai:gpt-5-mini",
            embed_model="openai:text-embedding-3-small",
        )
        answer = str(hook.get_chat_model().invoke("In one sentence: what is Titan?").content)
        vector = hook.get_embedding_model().embed_query("Titan")
        print(f"Answer: {answer}")
        print(f"Embedding dimensions: {len(vector)}")
        return {"answer": answer, "embedding_dimensions": len(vector)}

    chat_and_embed()


example_langchain_hook()
