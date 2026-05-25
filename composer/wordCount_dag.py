from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime

default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

dag = DAG(
    'wordcount_dataflow',
    default_args=default_args,
    description='Ejecutar job de Dataflow desde template',
    schedule_interval=None,
    catchup=False,
)

dataflow_task = DataflowTemplatedJobStartOperator(
    task_id='ejecutar_wordcount',
    template='gs://gcs-bucket-curso-05/templates/wordcount_template',
    location='us-central1',
    project_id='gcp-data-engineer-curso-05',
    dag=dag,
)