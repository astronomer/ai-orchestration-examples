"""
## example_openai_agents_durable

`run_durably_sync` runs an OpenAI Agents SDK agent so that a task retry
replays the model and tool calls the failed try already completed, instead of
running them again. On Airflow 3.3 and later the journal is kept in the task
state store. Once a run succeeds, the journal is deleted.

To show the replay, the agent's last step is `publish_answer`, which simulates
a worker crash on try 1 by exiting the task process. On try 2 the model calls
and the SQL tool calls replay from the journal (no `Running statement` lines
in the log), and only `publish_answer` runs live.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.sdk import BaseHook, dag, task

from include.airflow_try import task_try_number
from include.seed import seed_primary


def publish_answer(answer: str) -> str:
    """Publish the final answer. Call this once, with the complete answer."""
    if task_try_number() == 1:
        print("Simulating a worker crash on try 1")
        os._exit(1)
    print(f"Published on try {task_try_number()}: {answer}")
    return "published"


@dag(
    tags=["common-ai", "feature-example", "openai-agents", "durable"],
    doc_md=__doc__,
)
def example_openai_agents_durable():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def run_durable_agent() -> str:
        from agents import Agent, OpenAIChatCompletionsModel, RunConfig, function_tool
        from openai import AsyncOpenAI

        from airflow.providers.common.ai.durable.openai_agents import run_durably_sync
        from airflow.providers.common.ai.tools.openai_agents import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        api_key = BaseHook.get_connection("pydanticai_default").password
        agent = Agent(
            name="fleet_analyst",
            model=OpenAIChatCompletionsModel(model="gpt-5-mini", openai_client=AsyncOpenAI(api_key=api_key)),
            instructions=(
                "You are a fleet analyst. Use the SQL tools to answer, then call publish_answer "
                "with your answer, then reply with the answer."
            ),
            tools=[
                *AirflowTools(SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"])),
                function_tool(publish_answer),
            ],
        )
        result = run_durably_sync(
            agent,
            "How many spacecraft does each manufacturer have?",
            run_config=RunConfig(tracing_disabled=True),
        )
        return result.final_output

    seed_database() >> run_durable_agent()


example_openai_agents_durable()
