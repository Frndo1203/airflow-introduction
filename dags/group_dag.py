from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from subdags.subdags_download import subdag_downloads
from subdags.subdags_transform import subdag_transform

with DAG(dag_id='group_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    args = {'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup}

    download_subdag = SubDagOperator(
        task_id='download_subdag',
        subdag=subdag_downloads(dag.dag_id, 'download_subdag', args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_subdag = SubDagOperator(
        task_id='transform_subdag',
        subdag=subdag_transform(dag.dag_id, 'transform_subdag', args)
    )

    download_subdag >> check_files >> transform_subdag
