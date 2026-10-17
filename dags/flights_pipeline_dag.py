from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ImportError:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'ml-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'flights_data_pipeline',
    default_args=default_args,
    description='Automated Spark data pipeline: extracts from PostgreSQL, enriches from MinIO, cleans, and stores Parquet',
    schedule_interval=None,  # Trigger on-demand or set to '@daily'
    catchup=False,
    tags=['mlops', 'chapter09', 'flights', 'spark'],
) as dag:

    # Task 1: Merge flights data from PostgreSQL with MinIO airport lookup CSVs
    merge_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/jupyter/pyspark-notebook:latest',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'ulimit -u 65535 2>/dev/null || true && '
            'export PATH="/opt/conda/bin:$PATH" && '
            'export _JAVA_OPTIONS="-Xss512k" && '
            'export MALLOC_ARENA_MAX=2 && '
            'python -m pip install -q pyspark psycopg2-binary minio && '
            'rm -rf /tmp/app && '
            'git clone --depth 1 https://github.com/JacobItopa/dag-airflow.git /tmp/app && '
            'cd /tmp/app && '
            'python scripts/merge_data.py'
        ],
        security_context={'run_as_user': 0},
        labels={'app': 'spark-merge-task'},
        name='spark-merge-flights',
        task_id='merge_flights_data',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task 2: Clean the merged dataset, drop constant columns, and compute DELAYED label
    clean_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/jupyter/pyspark-notebook:latest',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'ulimit -u 65535 2>/dev/null || true && '
            'export PATH="/opt/conda/bin:$PATH" && '
            'export _JAVA_OPTIONS="-Xss512k" && '
            'export MALLOC_ARENA_MAX=2 && '
            'python -m pip install -q pyspark psycopg2-binary minio && '
            'rm -rf /tmp/app && '
            'git clone --depth 1 https://github.com/JacobItopa/dag-airflow.git /tmp/app && '
            'cd /tmp/app && '
            'python scripts/clean_data.py'
        ],
        security_context={'run_as_user': 0},
        labels={'app': 'spark-clean-task'},
        name='spark-clean-flights',
        task_id='clean_flights_data',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task dependency: merge first, clean second
    merge_task >> clean_task
