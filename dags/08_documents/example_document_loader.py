"""
## example_document_loader

`DocumentLoaderOperator` parses files into a list of `{"text", "metadata"}`
documents, ready for chunking and embedding. It infers the format from the
file suffix (Markdown, text, PDF, DOCX, JSON and more) and reads from local
paths or object storage. Here it reads the mission logs in
`include/mission_logs`.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "DocumentLoaderOperator", "rag"],
    doc_md=__doc__,
)
def example_document_loader():
    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/usr/local/airflow/include/mission_logs/*",
        file_extensions=[".md"],
        metadata_fields={"corpus": "mission_logs"},
    )

    @task
    def print_documents(docs: list[dict]) -> None:
        print(f"Loaded {len(docs)} documents")
        for doc in docs:
            print(f"  {doc['metadata'].get('file_path')}: {doc['text'][:60]!r}")

    print_documents(load_docs.output)


example_document_loader()
