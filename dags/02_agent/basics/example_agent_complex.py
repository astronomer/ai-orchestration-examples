"""
## example_agent_complex

Many `@task.agent` / `AgentOperator` parameters combined in one file:
`SQLToolset` against the seeded `space_logistics` database,
`enable_tool_logging`, `agent_params`, `usage_limits`, `durable` step
caching, a tracked `message_history` session, and `serialize_output`.

`enable_hitl_review` and its settings are written out but switched off,
because `enable_hitl_review=True` can't be combined with `durable=True` or
with `message_history`. Same Ceres supply run as `example_agent_basic`, with
the tools and the parameters added.

Not set here: `fallback_conn_ids`, `capabilities`, `cache_prompt`,
`emit_message_history` and the `tool_approval_*` parameters.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task
from pydantic_ai.usage import UsageLimits

from include.models import MissionPlan
from include.seed import seed_primary


@dag(
    tags=[
        "common-ai",
        "feature-example",
        "task.agent",
        "sql-toolset",
        "durable",
        "usage-limits",
        "tool-logging",
        "message-history",
        "structured-output",
    ],
    doc_md=__doc__,
)
def example_agent_complex():
    @task
    def prepare_input() -> dict:
        counts = seed_primary()
        print(f"Seeded: {counts}")
        return {
            "mission_name": "Ceres supply run",
            "origin": "Earth Spacedock",
            "destination": "Ceres Depot",
            "cargo_tonnes": 180,
        }

    @task.agent(
        llm_conn_id="pydanticai_default",
        model_id=None,
        system_prompt=(
            "You are a mission planner for an interplanetary freight line. "
            "Use the SQL tools to find a spacecraft in the spacecraft table "
            "whose capacity_tonnes covers the mission's cargo_tonnes and "
            "whose current_status is 'active', then return a structured "
            "MissionPlan whose steps name that spacecraft."
        ),
        output_type=MissionPlan,
        toolsets=[
            SQLToolset(
                db_conn_id="space_logistics",
                allowed_tables=["spacecraft"],
                max_rows=25,
            )
        ],
        enable_tool_logging=True,
        agent_params={
            "name": "ceres_mission_planner",
            "end_strategy": "exhaustive",
        },
        usage_limits=UsageLimits(
            request_limit=15,
            tool_calls_limit=8,
            total_tokens_limit=50_000,
        ),
        durable=True,
        # "[]" starts a tracked session: the full transcript gets pushed to
        # this task's XCom under key "message_history" for a follow-up turn
        # to resume. The default, None, is a single-turn run with no push.
        message_history="[]",
        # Mutually exclusive with durable=True and with message_history being
        # set (AgentOperator raises ValueError for either combination) --
        # left off here. See example_agent_hitl_review.py for a working demo.
        enable_hitl_review=False,
        max_hitl_iterations=3,
        hitl_timeout=timedelta(minutes=5),
        hitl_poll_interval=5.0,
        serialize_output=True,
    )
    def plan_mission(brief: dict) -> str:
        return (
            "Plan this mission step-by-step, including a refueling stop and "
            "a realistic estimated_days.\n\n"
            f"Brief: {brief}"
        )

    @task
    def consume_output(plan: MissionPlan | dict) -> None:
        if isinstance(plan, dict):
            plan = MissionPlan.model_validate(plan)
        print(f"{plan.mission_name}: {plan.origin} -> {plan.destination}")
        print(f"Estimated days: {plan.estimated_days}")
        for i, step in enumerate(plan.steps, start=1):
            print(f"  {i}. {step}")

    consume_output(plan_mission(prepare_input()))


example_agent_complex()
