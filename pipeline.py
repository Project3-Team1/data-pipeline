
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models.variable import Variable

from src.extract_load import extract_load_area_info



dag = DAG(
    dag_id='Data-Pipeline',
    start_date=datetime(2024,11,22),
    catchup=False,
    tags=['project'],
    schedule='0 * * * *')


extract_load_area_info = PythonOperator(
    task_id = 'extract_load_area_info',
    #python_callable param points to the function you want to run 
    python_callable = extract_load_area_info,
    op_kwargs={"metadata":Variable.get("area_list", deserialize_json=True), "API_KEY":'694f44565773747936374a62587a66'},
    #dag param points to the DAG that this task is a part of
    dag = dag)

# load_area_info = PythonOperator(
#     task_id = 'load_area_info',
#     #python_callable param points to the function you want to run 
#     python_callable = load_area_info,
#     op_args=["{{ ti.xcom_pull(task_ids='extract_area_info') }}"],
#     #dag param points to the DAG that this task is a part of
#     dag = dag)


#Assign the order of the tasks in our DAG
extract_load_area_info