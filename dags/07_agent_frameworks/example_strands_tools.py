"""
## example_strands_tools

A Strands Agents agent with a Common AI toolset as its tools. The
`AirflowTools` plugin wraps `SQLToolset`, so the agent gets read-only SQL
validation, row limits and Airflow's secret masking on every tool result.

The model is OpenAI's `gpt-5-mini` through Strands' `OpenAIModel`, with the
API key from the `pydanticai_default` connection.
"""

from __future__ import annotations

from airflow.sdk import BaseHook, dag, task

from include.seed import seed_primary


@dag(
    tags=["common-ai", "feature-example", "strands", "sql-toolset"],
    doc_md=__doc__,
)
def example_strands_tools():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task
    def run_strands_agent() -> str:
        from strands import Agent
        from strands.models.openai import OpenAIModel

        from airflow.providers.common.ai.tools.strands import AirflowTools
        from airflow.providers.common.ai.tools.tracing import agent_framework_tracing
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        api_key = BaseHook.get_connection("pydanticai_default").password
        model = OpenAIModel(client_args={"api_key": api_key}, model_id="gpt-5-mini")
        with agent_framework_tracing():
            agent = Agent(
                model=model,
                plugins=[AirflowTools(SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"]))],
                system_prompt="You are a fleet analyst. Use the SQL tools to answer.",
                callback_handler=None,
            )
            answer = str(agent("Which spacecraft are in maintenance?"))
        print(answer)
        return answer

    seed_database() >> run_strands_agent()


example_strands_tools()
