"""
## example_decision_policy

`DecisionPolicy` (experimental) on `@task.llm`. With `min_confidence` set,
the task returns the model's answer by itself only if the model reports a
confidence at or above the bar. Otherwise `on_uncertain="review"` opens a
human review on the Required Actions page, and `on_uncertain="fail"` fails the
task.

Text models such as OpenAI's `gpt-5-mini` don't report a confidence, so their
answers always count as uncertain. With this Dag's connection, every run goes
to review.
"""

from __future__ import annotations

from airflow.providers.common.ai.policies.decision import DecisionPolicy
from airflow.sdk import dag, task


@dag(
    tags=["common-ai", "feature-example", "DecisionPolicy", "task.llm"],
    doc_md=__doc__,
)
def example_decision_policy():
    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Answer with exactly one word: approve or reject.",
        decision_policy=DecisionPolicy(min_confidence=0.9, on_uncertain="review"),
    )
    def approve_cargo() -> str:
        return (
            "Cargo request: 12 tonnes of water ice from Ceres Depot to Mars "
            "Orbital, ship capacity 180 tonnes. Approve or reject?"
        )

    @task
    def print_decision(decision: str) -> None:
        print(f"Decision: {decision}")

    print_decision(approve_cargo())


example_decision_policy()
