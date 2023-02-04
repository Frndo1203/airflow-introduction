from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from groups.group_download import download_tasks
from groups.group_transforms import transform_tasks

with DAG(dag_id='group_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    args = {'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup}

    download_subdag = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_subdag = transform_tasks()

    download_subdag >> check_files >> transform_subdag
