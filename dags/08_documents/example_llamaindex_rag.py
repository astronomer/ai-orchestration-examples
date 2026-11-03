"""
## example_llamaindex_rag

Retrieval with LlamaIndex in three operators:

1. `DocumentLoaderOperator` parses the mission logs.
2. `LlamaIndexEmbeddingOperator` chunks and embeds them with OpenAI's
   `text-embedding-3-small` and persists the index to a folder.
3. `LlamaIndexRetrievalOperator` loads the index and returns the chunks most
   similar to the question.

The `llamaindex_default` connection has no API key, so the OpenAI client reads
`OPENAI_API_KEY` from the environment.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator
from airflow.providers.common.ai.operators.llamaindex_embedding import LlamaIndexEmbeddingOperator
from airflow.providers.common.ai.operators.llamaindex_retrieval import LlamaIndexRetrievalOperator
from airflow.sdk import dag, task

INDEX_DIR = "/tmp/llamaindex/mission_logs"


@dag(
    params={"question": "Which mission had a docking problem, and what caused it?"},
    tags=["common-ai", "feature-example", "llamaindex", "rag"],
    doc_md=__doc__,
)
def example_llamaindex_rag():
    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/usr/local/airflow/include/mission_logs/*",
        file_extensions=[".md"],
    )

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed",
        documents=load_docs.output,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        chunk_size=256,
        chunk_overlap=20,
        persist_dir=INDEX_DIR,
    )

    retrieve = LlamaIndexRetrievalOperator(
        task_id="retrieve",
        query="{{ params.question }}",
        index_persist_dir=INDEX_DIR,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        top_k=2,
    )

    @task
    def print_matches(matches) -> None:
        print(matches)

    embed >> retrieve
    print_matches(retrieve.output)


example_llamaindex_rag()
