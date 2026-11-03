"""
## example_llm_fallback

`fallback_conn_ids` lists backup connections. When the model provider in
`llm_conn_id` can't be reached, the task tries the backups in order.

To show this, `llm_primary_down` points at an address where no server runs, so
it always fails, like a provider outage. Every run then uses
`pydanticai_default` instead, and the task log names the model that answered.

You can also set `fallback_conn_ids` in the extra of the first connection
instead of in the Dag.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "task.llm", "fallback"],
    doc_md=__doc__,
)
def example_llm_fallback():
    @task.llm(
        llm_conn_id="llm_primary_down",
        fallback_conn_ids=["pydanticai_default"],
        system_prompt="Answer in one sentence.",
    )
    def summarize() -> str:
        return "Why do interplanetary cargo routes change with planetary alignment?"

    summarize()


example_llm_fallback()
