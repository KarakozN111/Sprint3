from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
sys.path.append(os.path.join(airflow_home, 'plugins'))

from scripts.etl_loader import (
    load_csv_to_raw_layer, 
    load_csv_to_raw_incremental,
    transform_raw_to_dwh_layer,
    run_data_quality_checks,
    publish_to_mart_layer,
    log_dag_start,
    log_dag_end
)

db_url = "postgresql://intern:intern@postgres:5432/postgres"

def on_dag_success_context(context):
    log_dag_end(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id'],
        status='SUCCESS',
        db_url=db_url
    )

def on_dag_failure_context(context):
    log_dag_end(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id'],
        status='FAILED',
        db_url=db_url,
        error_msg=str(context.get('exception', 'Неизвестная ошибка в таске'))
    )

default_args = {
    'owner': 'karakoz',
    'depends_on_past': False, # Ключевой параметр логики повторного запуска: позволяет перезапускать DAG независимо от прошлых падений
    'start_date': datetime(2026, 1, 1),
    'retries': 1,             # Авто-повтор шага при кратковременных сбоях сети/БД
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sprint3_batch_etl_pipeline',
    default_args=default_args,
    description='Пайплайн Batch ETL со статусной таблицей прогонов',
    schedule_interval=None,  
    catchup=False,
    on_success_callback=on_dag_success_context, # Триггер на успех
    on_failure_callback=on_dag_failure_context  # Триггер на падение
) as dag:

    task_init_run = PythonOperator(
        task_id='init_etl_run',
        python_callable=log_dag_start,
        op_kwargs={'dag_id': 'sprint3_batch_etl_pipeline', 'run_id': '{{ run_id }}', 'db_url': db_url}
    )

    task_load_students = PythonOperator(
        task_id='load_students_to_raw',
        python_callable=load_csv_to_raw_layer,
        op_kwargs={'file_path': '/opt/airflow/datasets/students.csv', 'table_name': 'students', 'db_url': db_url}
    )

    task_load_student_marks = PythonOperator(
        task_id='load_student_marks_to_raw',
        python_callable=load_csv_to_raw_incremental,
        op_kwargs={'file_path': '/opt/airflow/datasets/student_marks.csv', 'table_name': 'student_marks', 'db_url': db_url}
    )

    task_transform = PythonOperator(
        task_id='transform_raw_to_dwh',
        python_callable=transform_raw_to_dwh_layer,
        op_kwargs={'db_url': db_url}
    )

    task_quality_check = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks,
        op_kwargs={'db_url': db_url}
    )

    task_publish = PythonOperator(
        task_id='publish_to_marts',
        python_callable=publish_to_mart_layer,
        op_kwargs={'db_url': db_url}
    )


    # лог старта -> грузим данные -> трансформируем -> проверяем качество -> публикуем витрину
    task_init_run >> [task_load_students, task_load_student_marks] >> task_transform >> task_quality_check >> task_publish
