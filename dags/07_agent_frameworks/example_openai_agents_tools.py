"""
## example_openai_agents_tools

An OpenAI Agents SDK agent with a Common AI toolset as its tools.
`AirflowTools` wraps `SQLToolset`, so the agent gets read-only SQL validation,
row limits and Airflow's secret masking on every tool result.
`AirflowTraceProcessor` records the SDK's traces as the task's OpenTelemetry
spans instead of sending them to OpenAI.

The API key comes from the `pydanticai_default` connection.
"""

from __future__ import annotations

from airflow.sdk import BaseHook, dag, task

from include.seed import seed_primary


@dag(
    tags=["common-ai", "feature-example", "openai-agents", "sql-toolset"],
    doc_md=__doc__,
)
def example_openai_agents_tools():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task
    def run_openai_agent() -> str:
        from agents import Agent, OpenAIChatCompletionsModel, Runner, set_trace_processors
        from openai import AsyncOpenAI

        from airflow.providers.common.ai.tools.openai_agents import AirflowTools, AirflowTraceProcessor
        from airflow.providers.common.ai.tools.tracing import agent_framework_tracing
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        set_trace_processors([AirflowTraceProcessor()])
        api_key = BaseHook.get_connection("pydanticai_default").password
        agent = Agent(
            name="fleet_analyst",
            model=OpenAIChatCompletionsModel(model="gpt-5-mini", openai_client=AsyncOpenAI(api_key=api_key)),
            instructions="You are a fleet analyst. Use the SQL tools to answer.",
            tools=AirflowTools(SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"])),
        )
        with agent_framework_tracing():
            answer = Runner.run_sync(agent, "Which spacecraft has the largest capacity_tonnes?").final_output
        print(answer)
        return answer

    seed_database() >> run_openai_agent()


example_openai_agents_tools()
