from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "spark_pi",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf", "spark", "pi"],
    params={
        "airgap_registry_url": Param(
            "",
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="Airgap registry url. Trailing slash in the end is required",
        ),
    },
    render_template_as_native_obj=True,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
)

x = 'Teste concluído, envio completo'
print(x)