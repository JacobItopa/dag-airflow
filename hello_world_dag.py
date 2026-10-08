from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Define the overall pipeline (DAG)
with DAG(
    dag_id="hello_world_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # This means it only runs when you manually click "Play"
    catchup=False,
) as dag:

    # 2. Define the first box (hello.py)
    run_hello = BashOperator(
        task_id="hello_task",
        bash_command="python /opt/airflow/dags/repo/hello.py",
    )

    # 3. Define the second box (world.py)
    run_world = BashOperator(
        task_id="world_task",
        bash_command="python /opt/airflow/dags/repo/world.py",
    )

    # 4. Connect the boxes! (This replaces dragging the circle in the GUI)
    run_hello >> run_world
