from __future__ import annotations

import pendulum
import os
import sys
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

DAG_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_FOR_IMPORT = os.path.abspath(os.path.join(DAG_FILE_DIR, '..')) 
if PROJECT_ROOT_FOR_IMPORT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_FOR_IMPORT)

from Airflow.src.data_collection import run_data_collection
from Airflow.src.data_preprocessing import run_data_preprocessing
from Airflow.src.train_model import run_model_training

AIRFLOW_PROJECT_WORKING_DIR = DAG_FILE_DIR

with DAG(
    dag_id="train_pipe",
    start_date=pendulum.now(),
    concurrency=4,
    schedule_interval=timedelta(minutes=5),
    max_active_runs=1,
    catchup=False,
) as dag:

    data_collection_task = PythonOperator(
        task_id="data_collection",
        python_callable=run_data_collection,
        op_kwargs={"base_path": AIRFLOW_PROJECT_WORKING_DIR},
        dag=dag
    )

    data_preprocessing_task = PythonOperator(
        task_id="data_preprocessing",
        python_callable=run_data_preprocessing,
        op_kwargs={"base_path": AIRFLOW_PROJECT_WORKING_DIR},
        dag=dag
    )

    model_training_task = PythonOperator(
        task_id="model_training",
        python_callable=run_model_training,
        op_kwargs={"base_path": AIRFLOW_PROJECT_WORKING_DIR},
        dag=dag
    )

    data_collection_task >> data_preprocessing_task >> model_training_task
