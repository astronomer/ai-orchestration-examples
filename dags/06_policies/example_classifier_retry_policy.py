"""
## example_classifier_retry_policy

`ClassifierRetryPolicy` asks the model to pick one category for the
exception, such as `network`, `auth` or `rate_limit`. Whether that category is
retried, and after how long, comes from the `ErrorCategory` table in the Dag
code, not from the model. This Dag uses the default categories.

The task raises a connection reset on the first try, which the model should
classify as `network` (retried after 10 seconds), and succeeds on the second.
"""

from __future__ import annotations

from airflow.providers.common.ai.policies.retry import ClassifierRetryPolicy
from airflow.sdk import dag, task
from pendulum import duration

classifier_policy = ClassifierRetryPolicy(llm_conn_id="pydanticai_default")


@dag(
    tags=["common-ai", "feature-example", "ClassifierRetryPolicy", "retry-policy"],
    doc_md=__doc__,
)
def example_classifier_retry_policy():
    @task(retries=2, retry_delay=duration(seconds=5), retry_policy=classifier_policy)
    def fetch_telemetry(**context) -> str:
        if context["ti"].try_number == 1:
            raise ConnectionResetError(
                "[Errno 104] Connection reset by peer while reading from telemetry.ceres.internal:443"
            )
        return f"Telemetry fetched on try {context['ti'].try_number}"

    fetch_telemetry()


example_classifier_retry_policy()
