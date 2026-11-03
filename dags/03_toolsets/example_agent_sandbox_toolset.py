"""
## example_agent_sandbox_toolset

`SandboxToolset` gives the agent a sandbox to run commands and code in, off the
Airflow worker: `run_command`, `write_file`, `read_file` and more. The sandbox
is created on the agent's first tool call and destroyed when the run ends.

This Dag uses the `OpenSandboxBackend` against the `opensandbox-server` service
in `docker-compose.override.yml`, which starts each sandbox as a container on
your local Docker. The default `SandboxSpec` denies all network egress; the
server enforces that with its egress sidecar in `dns+nft` mode.

Needs the `opensandbox` extra of the provider.
"""

from __future__ import annotations

from airflow.providers.common.ai.sandbox import OpenSandboxBackend, SandboxSpec
from airflow.providers.common.ai.toolsets import SandboxToolset
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "task.agent", "sandbox-toolset"],
    doc_md=__doc__,
)
def example_agent_sandbox_toolset():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a flight engineer. Write and run Python in the sandbox to compute answers "
            "instead of doing arithmetic in your head. The sandbox has Python 3.12 and no network."
        ),
        toolsets=[
            SandboxToolset(
                OpenSandboxBackend(opensandbox_conn_id="opensandbox_default"),
                spec=SandboxSpec(block_network=True),
            )
        ],
    )
    def compute_transfer_window() -> str:
        return (
            "Earth orbits the Sun in 365.25 days and Mars in 686.98 days. Compute the synodic "
            "period in days, and how many Earth-Mars transfer windows open between 2027 and 2040."
        )

    compute_transfer_window()


example_agent_sandbox_toolset()
