"""
## example_agent_capabilities

`capabilities=` on `@task.agent` takes pydantic-ai capabilities, which hook
into the agent run. This Dag passes `Thinking(effort="high")`, which asks the
model to reason before it answers. For OpenAI reasoning models such as
`gpt-5-mini`, pydantic-ai sends it as the reasoning effort setting.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from pydantic_ai.capabilities import Thinking


@dag(
    tags=["common-ai", "feature-example", "task.agent", "capabilities"],
    doc_md=__doc__,
)
def example_agent_capabilities():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a careful flight planner. Answer in two sentences.",
        capabilities=[Thinking(effort="high")],
    )
    def plan_fuel_stop() -> str:
        return (
            "A ship burns 2.5 tonnes of fuel per day and carries 30 tonnes. "
            "The trip from Earth to Ceres takes 14 days. Does it need a fuel stop, "
            "and if so, after how many days at the latest?"
        )

    plan_fuel_stop()


example_agent_capabilities()
