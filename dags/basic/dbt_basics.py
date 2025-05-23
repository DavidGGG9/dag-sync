import os
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# IMPORT YOUR PACKAGES HERE


DBT_DIR = os.getenv("DBT_DIR")


with DAG(
    dag_id= "dbt_basics",
    start_date= datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days= 1),
    schedule= "@daily",
    catchup= False,
    default_args={
        'depends_on_past' : True
    },
) as dag:

    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command= f'dbt run --project-dir {DBT_DIR}',
    )

    dbt_test = BashOperator(
        task_id = 'dbt_test',
        bash_command= f'dbt test --project-dir {DBT_DIR}',
    )

    dbt_run >> dbt_test
