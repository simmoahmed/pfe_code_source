from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta

# DAG configuration
default_args = {
    'owner': 'elmaadoudi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'restAPI_to_Atlas',
    default_args=default_args,
    #description='A simple DAG to run a script once',
    schedule="@once",  # Changed from schedule_interval to schedule
    start_date=pendulum.today('UTC').add(days=-1),  # Changed from days_ago(1)
    tags=['example'],
)

# Define the task
run_script = BashOperator(
    task_id='run_script',
    bash_command='python /opt/airflow/dags/code/ExceltoAtlas.py',
    dag=dag,
)

# Task ordering
run_script