from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from datetime import datetime


# DAG configuration
default_args = {
    'owner': 'elmaadoudi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'excel_to_hive',
    default_args=default_args,
    description='A DAG to process Excel files into Hive',
    schedule_interval=timedelta(minutes=60),  # Adjust as necessary
    start_date=datetime(2024, 3, 15),
    catchup=False,
)

# Define the task
process_files = BashOperator(
    task_id='Excel_to_Hive',
    bash_command='python /opt/airflow/dags/code/monitoring_excel.py',
    dag=dag,
)

# Task ordering
process_files