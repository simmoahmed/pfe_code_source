from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
import os


def check_email_sent(**kwargs):
    # Chemin vers le fichier qui indique si l'email a été envoyé
    flag_file = '/opt/airflow/dags/data/email_sent.flag'

    # Vérifie si le fichier existe
    if os.path.exists(flag_file):
        # Le fichier existe, donc l'email a déjà été envoyé
        return False
    else:
        open(flag_file, 'a').close()
        return True
    return True

default_args = {
    'owner': 'elmaadoudi',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
    'email': ['elmaadoudimohamed1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'MetadataQualityProcess_Kafka',
    default_args=default_args,
    description='A simple DAG with parallel and sequential tasks',
    schedule_interval="0 6 2 * *",
    catchup=False,
) as dag:

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
        bash_command='python /opt/airflow/dags/code/Metadata/merge_kafka.py',
    )

    MetadataQualityCheck = BashOperator(
        task_id='quality_check',
        bash_command='python /opt/airflow/dags/code/Metadata/MetadataQualityCheck.py',
    )

    check_notification = ShortCircuitOperator(
        task_id='check_notification',
        python_callable=check_email_sent,
    )

    mecanisme_notification = BashOperator(
        task_id='mecanisme_notif',
        bash_command='python /opt/airflow/dags/code/Metadata/mail.py',
        trigger_rule="all_done", 
    )

    [extract_column, extract_tab] >> merge_column_tab >> MetadataQualityCheck >> check_notification >> mecanisme_notification
