"""
## example_agent_skills_toolset

`AgentSkillsToolset` gives the agent Agent Skills: folders with a `SKILL.md`
that the model discovers and loads on demand. This Dag reads skills from the
local `include/skills` folder; `GitSkills(repo_url=...)` reads them from a Git
repository at task run time instead.

The `cargo-report` skill sets a house style for cargo reports, so the answer
follows it only if the agent loaded the skill.

Needs the `skills` extra of the provider.
"""

from __future__ import annotations

from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset
from airflow.sdk import dag, task

SKILLS_DIR = "/usr/local/airflow/include/skills"


@dag(
    tags=["common-ai", "feature-example", "task.agent", "skills-toolset"],
    doc_md=__doc__,
)
def example_agent_skills_toolset():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You work in operations at Cosmic Freight. Check your skills before answering.",
        toolsets=[AgentSkillsToolset(sources=[SKILLS_DIR], exclude_tools={"run_skill_script"})],
    )
    def write_cargo_report() -> str:
        return (
            "Write a cargo report: shipment SHP-1001 from Earth Spacedock to Ceres Depot "
            "was rerouted around solar flares on 2026-07-02 and is now 4 days late."
        )

    write_cargo_report()


example_agent_skills_toolset()
