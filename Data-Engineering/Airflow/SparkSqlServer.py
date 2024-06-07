from airflow import DAG
import pyodbc
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
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
    "SQLServer_Ezmeral",
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

def conectorServer():
    # Definir os parâmetros de conexão
    server = '172.16.0.179:1401'
    database = 'AdventureWorks2022'
    username = 'SA'
    password = '#Gf15533155708'
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Conectar ao banco de dados
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Escrever a consulta SQL
    query = "SELECT * FROM Sales.SalesOrderHeader"

    # Executar a consulta
    cursor.execute(query)

    # Puxar os dados
    rows = cursor.fetchall()

    # Exibir os resultados
    for row in rows:
        print(row)

    # Fechar a conexão
    cursor.close()
    connection.close()

# conectorSql = conectorServer (
#     task_id="conector",
#     application_file="SQLServer_Ezmeral.py",
#     do_xcom_push=True,
#     dag=dag,
#     api_group="sparkoperator.hpe.com",
#     enable_impersonation_from_ldap_user=True,
# )

conectorSql = PythonOperator(
    task_id="conector",
    python_callable=conectorServer,
    dag=dag,
)

conectorServer 