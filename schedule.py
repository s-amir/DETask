
"""
***SAMPLE***
"""


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'my_sample_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  # Schedule the DAG to run every 30 minutes
    catchup=False,
)

# Define a Python function to run your script
def run_my_script():
    # Replace 'your_script.py' with the path to your Python script
    subprocess.run(['python', 'your_script.py'])

# Create a PythonOperator to run the script
run_script_task = PythonOperator(
    task_id='run_my_script_task',
    python_callable=run_my_script,
    dag=dag,
)

# Define task dependencies (if any)
# For example, you can set dependencies like this:
# run_script_task >> another_task

if __name__ == "__main__":
    dag.cli()
