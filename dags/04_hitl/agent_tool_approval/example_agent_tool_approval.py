"""
## example_agent_tool_approval

Per-tool approval on `@task.agent`. A tool marked `requires_approval=True`
doesn't run when the agent calls it. The task pauses in the `awaiting_input`
state, and the pending call, with its arguments, shows up on the Required
Actions page. Approve runs the call; reject tells the agent it was denied.

`check_berth` runs without asking. `dispatch_spacecraft` needs approval.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import dag, task
from pydantic_ai import FunctionToolset, Tool


def check_berth(station: str) -> str:
    """Return whether a station has a free docking berth."""
    return f"{station} has 2 free berths."


def dispatch_spacecraft(spacecraft_id: str, destination: str) -> str:
    """Dispatch a spacecraft to a destination."""
    return f"{spacecraft_id} dispatched to {destination}."


@dag(
    tags=["common-ai", "feature-example", "task.agent", "tool-approval", "hitl"],
    doc_md=__doc__,
)
def example_agent_tool_approval():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="Use the tools. Check the berth before dispatching.",
        toolsets=[
            FunctionToolset(
                [Tool(check_berth), Tool(dispatch_spacecraft, requires_approval=True)]
            )
        ],
        tool_approval_timeout=timedelta(hours=1),
    )
    def dispatch() -> str:
        return "Dispatch SC-001 to Ceres Depot."

    dispatch()


example_agent_tool_approval()
