from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# Define the default arguments for the DAG
default_args = {
    'owner': 'elmaadoudi',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['elmaadoudimohamed1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Instantiate the DAG
with DAG(
    'MetadataQualityProcess',
    default_args=default_args,
    description='A simple DAG with parallel and sequential tasks',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the tasks using BashOperator
    extract_column = BashOperator(
        task_id='extract_column',
        bash_command='python /opt/airflow/dags/code/Metadata/extractINFOSCOL.py',
    )

    extract_tab = BashOperator(
        task_id='extract_tab',
        bash_command='python /opt/airflow/dags/code/Metadata/extractINFOSTAB.py',
    )

    merge_column_tab = BashOperator(
        task_id='merge_column_tab',
        bash_command='python /opt/airflow/dags/code/Metadata/merge_save.py',
    )

    MetadataQualityCheck = BashOperator(
        task_id='quality_check',
        bash_command='python /opt/airflow/dags/code/Metadata/MetadataQualityCheck.py',
    )

    mecanisme_notification = BashOperator(
        task_id='mecanisme_notif',
        bash_command='python /opt/airflow/dags/code/Metadata/mail.py',
    )

    [extract_column, extract_tab] >> merge_column_tab >> MetadataQualityCheck >> mecanisme_notification
