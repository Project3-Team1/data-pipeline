
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models.variable import Variable

from src.etl_data import extract_area_info, transform_area_info, load_area_info




def get_data(**context):
    raw_area_info, timestamp = extract_area_info(context["params"]["metadata"],
                                                 context["params"]["api_address"])
    area_info = transform_area_info(raw_area_info, timestamp)
    load_area_info(area_info, context["params"]["db_connection_info"])





dag = DAG(
    dag_id='Get-Data',
    start_date=datetime(2024,11,22),
    catchup=False,
    tags=['project'],
    schedule='0 * * * *')


get_area_info = PythonOperator(
    task_id = 'get_data',
    #python_callable param points to the function you want to run 
    python_callable = get_data,
    params={"metadata":Variable.get("area_list", deserialize_json=True),
            "api_address":Variable.get("api_address_key"),
            "db_connection_info":Variable.get("db_connection_info", deserialize_json=True)},
    #dag param points to the DAG that this task is a part of
    dag = dag)


#Assign the order of the tasks in our DAG
get_area_info


