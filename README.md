Common AI Provider Example Dags
===============================

Example Airflow Dags for
[`apache-airflow-providers-common-ai`](https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/) and
human-in-the-loop operators from the standard provider.

> See [Orchestrate AI tasks with Apache Airflow® and the Common AI provider](https://www.astronomer.io/docs/learn/airflow-common-ai-provider) and [Human-in-the-loop workflows with Airflow](https://www.astronomer.io/docs/learn/airflow-human-in-the-loop) for more information.

Astro Runtime 3.4 alpha (`runtime-dev:3.4-1-alpha6`, Airflow 3.4 pre-release) with
`apache-airflow-providers-common-ai` 1.1.0. Package versions are pinned in
`requirements.txt`.

How to use this repository
-----------

1. Copy `.env_example` to `.env` and add your own credentials for your model provider, adjust the model in `AIRFLOW_CONN_PYDANTICAI_DEFAULT` if needed.
2. Start Airflow

```bash
astro dev start
```

---

Layout
------

```
dags/
  01_llm_tasks/
    decorators/          one Dag per @task.llm* decorator
    operators/           classic-operator forms
  02_agent/
    basics/              @task.agent, from minimal to many parameters combined
    durable/             durable=True
    capabilities/        capabilities= and CodeMode
  03_toolsets/           one Dag per toolset, plus one that combines two
  04_hitl/
    standard_operators/  human-in-the-loop operators from the standard provider
    agent_review/        common-ai's enable_hitl_review
    agent_tool_approval/ per-tool approval on @task.agent
  05_connections_hooks/  the hook API and the MCP connection transports
  06_policies/           LLM-backed retry and decision policies
  07_agent_frameworks/   Common AI toolsets in LangChain, OpenAI Agents SDK and Strands agents
  08_documents/          document loading and LlamaIndex retrieval
  09_task_loops/         an agent inside an Airflow 3.4 task loop
  other/                 end-to-end pipelines built out of the above
  framework_comparison/  the same Dag six times, once per agent framework
```

### 01_llm_tasks

| Dag | Provider component |
|---|---|
| `example_llm_operator` | `LLMOperator` / `@task.llm` with a Pydantic `output_type` and `UsageLimits` |
| `example_llm_branch` | `LLMBranchOperator` / `@task.llm_branch` |
| `example_llm_sql_query` | `LLMSQLQueryOperator` / `@task.llm_sql` |
| `example_llm_file_analysis` | `LLMFileAnalysisOperator` / `@task.llm_file_analysis` |
| `example_llm_schema_compare` | `@task.llm_schema_compare` |
| `example_llm_batch` | `@task.llm_batch`, the provider's batch API with results written to object storage |
| `example_llm_schema_compare_operator` | `LLMSchemaCompareOperator` |
| `example_llm_usage_xcom` | Token usage and cost pushed to XCom under `usage` |

### 02_agent

| Dag | Provider component |
|---|---|
| `example_agent_basic` | `AgentOperator` / `@task.agent` with no toolset |
| `example_agent_complex` | `durable`, `usage_limits`, `message_history`, `agent_params`, tool logging |
| `example_agent_advanced` | A custom `AbstractToolset` |
| `example_agent_basic_durable` | `durable=True` |
| `example_agent_capabilities` | `capabilities=` with pydantic-ai's `Thinking` |
| `example_agent_code_mode` | `CodeMode` capability from `pydantic-ai-harness` |
| `example_agent_emit_message_history` | `emit_message_history=True` |

### 03_toolsets

| Dag | Toolset |
|---|---|
| `example_agent_sql_toolset` | `SQLToolset` |
| `example_agent_hook_toolset` | `HookToolset`. Any Airflow Hook's methods can become tools |
| `example_agent_datafusion_toolset` | `DataFusionToolset` |
| `example_agent_mcp_toolset` | `MCPToolset` |
| `example_agent_object_storage_toolset` | `ObjectStorageToolset` |
| `example_agent_skills_toolset` | `AgentSkillsToolset` with a local skill in `include/skills` |
| `example_agent_sandbox_toolset` | `SandboxToolset` with `OpenSandboxBackend`, against the local OpenSandbox server |
| `example_agent_logging_toolset` | Tool-call logging with `enable_tool_logging=True` |
| `example_agent_multi_toolset` | `SQLToolset` and `HookToolset` on one agent |

### 04_hitl

| Dag | Operator | Info |
|---|---|---|
| `example_hitl_approval` | `ApprovalOperator` | Approve or reject. Rejecting skips the downstream tasks |
| `example_hitl_operator` | `HITLOperator` | N options, `multiple=True` |
| `example_hitl_branch` | `HITLBranchOperator` | `options_mapping`  |
| `example_hitl_entry` | `HITLEntryOperator` | Input form |
| `example_agent_hitl_review` | common-ai `enable_hitl_review=True` | Note: HITL interaction in a separate plugin tab on the task instances |
| `example_agent_tool_approval` | common-ai `requires_approval=True` on a tool | The pending tool call shows up on the Required Actions page |

`example_agent_hitl_review` is the only Dag here that uses common-ai's React review plugin.
The plugin loads its JS bundle from `AIRFLOW__API__BASE_URL`, so if your Airflow
isn't on `http://localhost:8080` you need the base-URL block in
`docker-compose.override.yml`, which ships commented out. The four standard
operators work either way.

### 05_connections_hooks

| Dag | Provider component |
|---|---|
| `example_pydantic_ai_hook` | `PydanticAIHook.create_agent()` |
| `example_mcp_connection_transports` | `mcp` connection using stdio and http |
| `example_mcp_hook` | `MCPHook` used directly, without an agent |
| `example_langchain_hook` | `LangChainHook` chat and embedding models |
| `example_llm_fallback` | `fallback_conn_ids`: the first connection always fails, so the task uses the backup connection |

### 06_policies

| Dag | Provider component |
|---|---|
| `example_llm_retry_policy` | `LLMRetryPolicy` |
| `example_classifier_retry_policy` | `ClassifierRetryPolicy` |
| `example_decision_policy` | `DecisionPolicy` on `@task.llm` (experimental) |

### 07_agent_frameworks

| Dag | Provider component |
|---|---|
| `example_langchain_toolset_bridge` | `airflow_toolset_to_langchain_tools` with a LangChain `create_agent` agent |
| `example_openai_agents_tools` | OpenAI Agents SDK `AirflowTools` and `AirflowTraceProcessor` |
| `example_openai_agents_durable` | `run_durably_sync`: a retry replays the completed model and tool calls |
| `example_strands_tools` | Strands `AirflowTools` plugin |
| `example_strands_durable` | `invoke_durably`: a retry resumes from the last checkpoint |

Both durable Dags simulate a worker crash on try 1 to show the replay on try 2.

### 08_documents

| Dag | Provider component |
|---|---|
| `example_document_loader` | `DocumentLoaderOperator` on the Markdown files in `include/mission_logs` |
| `example_llamaindex_rag` | `LlamaIndexEmbeddingOperator` and `LlamaIndexRetrievalOperator` |

### 09_task_loops

| Dag | What it builds |
|---|---|
| `example_agent_text_to_sql_loop` | `@task.agent` in a task loop that retries until its SQL passes a check |

### other

| Dag | What it builds |
|---|---|
| `email_routing` | `@task.llm` sorts mail into P0-P4, `@task.branch` routes it |
| `support_reply_evals` | LLM as judge. Scores agent replies and customer reactions, CSAT|

---

Agent framework comparison
--------------------------------------------------

Six Dags run an identical support-ticket flow. Fetch a ticket, draft a
structured `TicketResponse` using a `lookup_shipment` tool, HITL review branch.

| Dag | Framework |
|---|---|
| `support_ticket_common_ai` | common-ai provider (`@task.agent`) |
| `support_ticket_pydantic_ai` | pydantic-ai used directly |
| `support_ticket_langgraph` | LangGraph ReAct agent |
| `support_ticket_crewai` | CrewAI single-agent crew, in a `@task.virtualenv` |
| `support_ticket_temporal` | Temporal workflow |
| `support_ticket_direct_api` | OpenAI SDK and a hand-written tool loop |

All examples other than the common AI one use `gpt-5-mini` with `OPENAI_API_KEY`, adjust for other model providers.

CrewAI requires `openai<3` and the Common AI provider needs `openai>=3`, so
`support_ticket_crewai` installs CrewAI and `openai` 2.x into a virtualenv at
task runtime instead of into the image, which adds install time to every run of
that task.

`example_agent_sandbox_toolset` uses the `opensandbox-server` service from
`docker-compose.override.yml` (OpenSandbox 1.2.0). The server mounts the host's Docker
socket and starts each sandbox as a container on your local Docker, with an egress
sidecar in `dns+nft` mode so the default deny-all network policy is enforced. The
server publishes no port and has no API key; only containers on the `airflow`
network reach it. Sandbox ports are published on `127.0.0.1` only.

The Temporal Dag uses the `temporal` service from `docker-compose.override.yml`. Its web UI is at `http://localhost:8233`.

---
