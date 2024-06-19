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
import jaydebeapi
import os

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
<<<<<<< HEAD
    # Definir os parâmetros de conexão
    server = '172.16.3.103:1433'
    database = 'AdventureWorksDW2019'
    username = 'sa'
    password = 'r2d2c3po*'
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
=======
>>>>>>> d99e01f299ec1014684f2cc31193df6f86fb6519

# Definindo a variável de ambiente JAVA_HOME (se ainda não estiver configurada)
os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk-22'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '\\bin;' + os.environ['PATH']

# Caminho para o arquivo .jar do driver JDBC (use barras duplas ou uma string bruta para evitar a invalid escape sequence)
driver_jar = r'C:\Program Files (x86)\sqljdbc_12.6\enu\jars\mssql-jdbc-12.6.2.jre8.jar'
driver_class = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

# URL de conexão JDBC para o SQL Server
jdbc_url = 'jdbc:sqlserver://172.16.0.179:1401;database=AdventureWorks2022;encrypt=false'

# Credenciais de acesso ao banco de dados
username = 'sa'
password = '#Gf15533155708'

# Conectando ao banco de dados
conn = jaydebeapi.connect(driver_class, jdbc_url, [username, password], driver_jar)

# Criando um cursor para executar consultas
cursor = conn.cursor()

# Executando uma consulta SQL
cursor.execute("SELECT * FROM sales.salesperson")

# Obtendo os resultados da consulta
results = cursor.fetchall()
for row in results:
    print(row)

# Fechando o cursor e a conexão
cursor.close()
conn.close()

conectorSql = conectorServer (
    task_id="submit",
    application_file="example_spark_pi.yaml",
    do_xcom_push=True,
    dag=dag,
)

conectorServer 