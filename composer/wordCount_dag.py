from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime

default_args = {
<<<<<<< HEAD
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
=======
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
>>>>>>> 4779e71c81452a1416f26793b22455e446ed3e64
