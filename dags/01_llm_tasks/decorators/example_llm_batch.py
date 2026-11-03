"""
## example_llm_batch

`@task.llm_batch` sends a list of prompts to the provider's batch API (OpenAI
or Anthropic, picked from the `model_id` prefix) instead of one request per
prompt. The results don't go to XCom. They are written as JSONL under
`result_path`, and the task returns a manifest that says where they are and how
many requests succeeded.

Batch jobs are asynchronous on the provider side, so this task can take
minutes or longer. `deferrable=True` hands the waiting to the triggerer.
"""

from __future__ import annotations

from airflow.sdk import ObjectStoragePath, dag, task


@dag(
    tags=["common-ai", "feature-example", "task.llm_batch"],
    doc_md=__doc__,
)
def example_llm_batch():
    @task
    def get_planets() -> list[str]:
        return ["Mars", "Europa", "Titan"]

    @task.llm_batch(
        llm_conn_id="pydanticai_default",
        result_path="file:///tmp/llm_batch/{{ dag.dag_id }}/{{ run_id }}",
        system_prompt="Answer in one sentence.",
        deferrable=True,
        poll_interval=30,
    )
    def describe_planets(planets: list[str]) -> list[str]:
        return [f"What is the biggest challenge of landing cargo on {p}?" for p in planets]

    @task
    def print_results(manifest: dict) -> None:
        print(f"Manifest: {manifest}")
        print(ObjectStoragePath(manifest["result_uri"]).read_text())

    print_results(describe_planets(get_planets()))


example_llm_batch()
