"""
## example_agent_object_storage_toolset

`ObjectStorageToolset` gives the agent read-only access to the files under
one path: `list_files`, `get_file_info` and `read_file`. The path can be
anything `ObjectStoragePath` opens (`s3://`, `gs://`, `file://` and others).
Here it is the local `include/fixtures/cargo_manifests` folder.
"""

from __future__ import annotations

from airflow.providers.common.ai.toolsets.object_storage import ObjectStorageToolset
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "task.agent", "object-storage-toolset"],
    doc_md=__doc__,
)
def example_agent_object_storage_toolset():
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="Use the file tools to answer. Answer in two sentences at most.",
        toolsets=[
            ObjectStorageToolset(path="file:///usr/local/airflow/include/fixtures/cargo_manifests")
        ],
    )
    def find_riskiest_manifest() -> str:
        return "Which cargo manifest has the highest risk, and why?"

    find_riskiest_manifest()


example_agent_object_storage_toolset()
