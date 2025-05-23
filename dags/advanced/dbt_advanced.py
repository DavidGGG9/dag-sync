import os
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
import re

# IMPORT YOUR PACKAGES HERE

DBT_DIR = os.getenv("DBT_DIR")


def load_manifest(file: str) -> dict:
    """
    Reads the json `file` and returns it as a dict.
    """
    with open(file) as f:
        data = json.load(f)
    return data


def make_dbt_task(node: str, dbt_verb: str) -> BashOperator:
    """
    Returns a BashOperator with a bash command to run or test the given node.
    Adds the project-dir argument and names the tasks as shown by the below examples.
    Cleans the node's name when it is a test.

    Examples:
    >>> print(make_dbt_task('model.dbt_lewagon.my_first_dbt_model', 'run'))
    BashOperator(
        task_id=model.dbt_lewagon.my_first_dbt_model,
        bash_command= "dbt run --models my_first_dbt_model --project-dir /app/airflow/dbt_lewagon"
    )

    >>> print(make_dbt_task('test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710', 'test'))
    BashOperator(
        task_id=test.dbt_lewagon.not_null_my_first_dbt_model_id,
        bash_command= "dbt test --models not_null_my_first_dbt_model_id --project-dir /app/airflow/dbt_lewagon"
    )
    """
    if dbt_verb == 'run':
        model_name = node.split('.')[-1]
        operator = BashOperator(
            task_id = node,
            bash_command= f'dbt {dbt_verb} --models {model_name} --project-dir {DBT_DIR}'
        )

    elif dbt_verb == 'test':
        model_name = node.split('.')[-2]
        operator = BashOperator(
            task_id = re.findall(r'^(.*)\.[^.]+$',node)[0],
            bash_command= f'dbt {dbt_verb} --models {model_name} --project-dir {DBT_DIR}'
        )

    return operator


def create_tasks(data: dict) -> dict:
    """
    This function should iterate through data["nodes"] keys and call make_dbt_task
    to build and return a new dict containing as keys all nodes' names and their corresponding dbt tasks as values.
    """
    mapping = {
        'model' : 'run',
        'test' : 'test'
    }

    first_dict = {node: data['nodes'][node]['resource_type'] for node in data['nodes']}
    second_dict = {key : mapping[value] for key, value in first_dict.items()}
    final_dict = {key : make_dbt_task(key, value) for key, value in second_dict.items()}

    return final_dict

def create_dags_dependencies(data: dict, dbt_tasks: dict):
    """
    Iterate over every node and their dependencies (by using data and the "depends_on" key)
    to order the Airflow tasks properly.
    """
    for key, value in dbt_tasks.items():
        parent_node = data['nodes'][key]['depends_on']['nodes']
        if len(parent_node) == 0:
            continue
        dbt_tasks[parent_node[0]] >> value


with DAG(
    "dbt_advanced",
    default_args={
        "depends_on_past": False,
    },
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@daily",
    catchup=True,
) as dag:
    with open(f"{DBT_DIR}/manifest.json") as f:
        data = json.load(f)
    dbt_tasks = create_tasks(data)
    create_dags_dependencies(data, dbt_tasks)
