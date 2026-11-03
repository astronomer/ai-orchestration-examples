"""
## example_llm_usage_xcom

`@task.llm` and the other LLM operators push the run's token usage and cost
to XCom under the key `usage`: requests, input, output and total tokens, tool
calls, cache tokens, and the cost in USD as a string (or `None` when the
model has no price). A downstream task can track spend without parsing logs.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "task.llm", "usage"],
    doc_md=__doc__,
)
def example_llm_usage_xcom():
    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Answer in one sentence.",
    )
    def describe_ceres() -> str:
        return "What is Ceres?"

    @task
    def print_usage(ti=None) -> None:
        usage = ti.xcom_pull(task_ids="describe_ceres", key="usage")
        print(f"Usage: {usage}")
        print(f"Total tokens: {usage['total_tokens']}, cost: {usage['cost']} USD")

    describe_ceres() >> print_usage()


example_llm_usage_xcom()
