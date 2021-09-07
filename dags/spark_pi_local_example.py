import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from main import run_spark_job

DAG_ID = os.path.basename(__file__).replace('.py', '')  # 'spark_pi_example'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

SPARK_STEPS = [
    {
        'Name': 'main',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
        },
    }
]

with DAG(
        dag_id=DAG_ID,
        description='Run built-in Spark app locally',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['local', 'spark']
) as dag:
    t1 = PythonOperator(
        task_id='python_run_id',
        python_callable=run_spark_job,
        provide_context=True,
        dag=dag
    )

    t1.run()
