"""
## example_mcp_hook

`MCPHook` used directly, without an agent. `get_conn()` returns a pydantic-ai
MCP toolset for the server in the `mcp_stdio_space` connection. The task lists
the server's tools and calls one of them.
"""

from __future__ import annotations

import asyncio

from airflow.providers.common.ai.hooks.mcp import MCPHook
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "MCPHook", "mcp"],
    doc_md=__doc__,
)
def example_mcp_hook():
    @task
    def call_mcp_tool() -> dict:
        async def run() -> dict:
            toolset = MCPHook(mcp_conn_id="mcp_stdio_space").get_conn()
            async with toolset:
                tools = await toolset.list_tools()
                print("Tools:", [t.name for t in tools])
                return await toolset.direct_call_tool("count_stations_by_planet", {})

        result = asyncio.run(run())
        print(f"count_stations_by_planet: {result}")
        return result

    call_mcp_tool()


example_mcp_hook()
