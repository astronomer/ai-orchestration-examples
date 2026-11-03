"""
## example_agent_code_mode

`CodeMode` from `pydantic-ai-harness`, passed as a capability. Instead of
calling tools one at a time, the model writes a short Python program that
calls them, and the program runs in a sandboxed interpreter. That saves model
round trips when an answer needs many tool calls.

Needs the `code-mode` extra of the provider, or `pydantic-ai-harness[codemode]`.
"""

from __future__ import annotations

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task
from pydantic_ai_harness import CodeMode

from include.seed import seed_primary


@dag(
    tags=["common-ai", "feature-example", "task.agent", "capabilities", "code-mode"],
    doc_md=__doc__,
)
def example_agent_code_mode():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a fleet analyst. Write Python that calls the tools to answer.",
        toolsets=[SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"])],
        capabilities=[CodeMode(max_tool_calls=50)],
    )
    def count_by_status() -> str:
        return "How many spacecraft are there per current_status, and which status is most common?"

    seed_database() >> count_by_status()


example_agent_code_mode()
