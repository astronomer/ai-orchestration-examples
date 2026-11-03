"""
## example_langchain_toolset_bridge

`airflow_toolset_to_langchain_tools` turns a Common AI toolset into LangChain
tools. A LangChain agent built with `create_agent` then queries the seeded
`space_logistics` database through `SQLToolset`, with its read-only SQL
validation and row limits.
"""

from __future__ import annotations

from airflow.providers.common.ai.hooks.langchain import LangChainHook
from airflow.providers.common.ai.toolsets import airflow_toolset_to_langchain_tools
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task

from include.seed import seed_primary


@dag(
    tags=["common-ai", "feature-example", "langchain", "sql-toolset"],
    doc_md=__doc__,
)
def example_langchain_toolset_bridge():
    @task
    def seed_database() -> None:
        print(f"Seeded: {seed_primary()}")

    @task
    def run_langchain_agent() -> str:
        from langchain.agents import create_agent

        tools = airflow_toolset_to_langchain_tools(
            SQLToolset(db_conn_id="space_logistics", allowed_tables=["spacecraft"])
        )
        model = LangChainHook(llm_conn_id="langchain_default", llm_model="openai:gpt-5-mini").get_chat_model()
        agent = create_agent(
            model,
            tools=tools,
            system_prompt="You are a fleet analyst. Use the SQL tools to answer.",
        )
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "How many spacecraft are currently available?"}]}
        )
        answer = result["messages"][-1].content
        print(answer)
        return answer

    seed_database() >> run_langchain_agent()


example_langchain_toolset_bridge()
