from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 5, 8),
}

with DAG(
    'dataflow_dag_with_operator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    launch_template = DataflowTemplatedJobStartOperator(
        task_id='launch_dataflow_template',
        template_path='gs://bucket_shakespeare_pi/templates/wordcount_template',
        parameters={
            'input': 'gs://dataflow-samples/shakespeare/kinglear.txt',
            'output': 'gs://bucket_shakespeare_pi/output/wordcount_from_airflow'
        },
        location='us-east1',
        proyect_id = "data-engineer-proyect-02",
        dag=dag,
    )