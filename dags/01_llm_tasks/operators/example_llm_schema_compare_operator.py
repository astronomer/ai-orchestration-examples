"""
## example_llm_schema_compare_operator

`LLMSchemaCompareOperator`, the classic-operator form of
`@task.llm_schema_compare`. It compares the `spacecraft` table in two SQLite
databases, where the alternate one has schema drift, and returns a report of
the mismatches.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm_schema_compare import (
    LLMSchemaCompareOperator,
)
from airflow.sdk import dag, task

from include.seed import seed_alt_with_drift, seed_primary


@dag(
    tags=["common-ai", "feature-example", "LLMSchemaCompareOperator"],
    doc_md=__doc__,
)
def example_llm_schema_compare_operator():
    @task
    def seed_databases() -> None:
        print(f"primary={seed_primary()}  alt={seed_alt_with_drift()}")

    compare_schemas = LLMSchemaCompareOperator(
        task_id="compare_schemas",
        llm_conn_id="pydanticai_default",
        prompt=(
            "Compare the given tables. Flag any mismatch that would break a CDC "
            "pipeline copying rows from the primary into the alternate warehouse."
        ),
        db_conn_ids=["space_logistics", "space_logistics_alt"],
        table_names=["spacecraft"],
    )

    @task
    def print_report(report: dict) -> None:
        print(f"Compatible: {report.get('compatible')}")
        for mismatch in report.get("mismatches", []) or []:
            print(f"  mismatch: {mismatch}")

    seed_databases() >> compare_schemas
    print_report(compare_schemas.output)


example_llm_schema_compare_operator()
