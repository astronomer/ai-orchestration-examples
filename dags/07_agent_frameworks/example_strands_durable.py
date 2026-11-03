"""
## example_strands_durable

`invoke_durably` runs a Strands agent so that a task retry resumes from the
last agent cycle it finished, instead of starting over. On Airflow 3.3 and
later the agent's state is kept in the task state store.

To show the resume, the agent's last step is `publish_answer`, which simulates
a worker crash on try 1 by exiting the task process. On try 2 the agent picks
up from its last checkpoint, so the SQL queries from try 1 don't run again
(no `Running statement` lines in the log).
"""

from __future__ import annotations

import os
from datetime import timedelta
from functools import partial

from airflow.sdk import BaseHook, dag, task

from include.airflow_try import task_try_number
from include.seed import seed_primary


@dag(
    tags=["common-ai", "feature-example", "strands", "durable"],
    doc_md=__doc__,
)
def example_strands_durable():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def run_durable_strands_agent() -> str:
        from strands import Agent, tool
        from strands.models.openai import OpenAIModel

        from airflow.providers.common.ai.durable.strands import invoke_durably
        from airflow.providers.common.ai.tools.strands import AirflowTools
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        @tool
        def publish_answer(answer: str) -> str:
            """Publish the final answer. Call this once, with the complete answer."""
            if task_try_number() == 1:
                print("Simulating a worker crash on try 1")
                os._exit(1)
            print(f"Published on try {task_try_number()}: {answer}")
            return "published"

        api_key = BaseHook.get_connection("pydanticai_default").password
        agent = partial(
            Agent,
            model=OpenAIModel(client_args={"api_key": api_key}, model_id="gpt-5-mini"),
            plugins=[AirflowTools(SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"]))],
            tools=[publish_answer],
            system_prompt=(
                "You are a fleet analyst. Use the SQL tools to answer, then call publish_answer "
                "with your answer, then reply with the answer."
            ),
            callback_handler=None,
        )
        return str(invoke_durably(agent, "What is the total capacity_tonnes of all available spacecraft?"))

    seed_database() >> run_durable_strands_agent()


example_strands_durable()
