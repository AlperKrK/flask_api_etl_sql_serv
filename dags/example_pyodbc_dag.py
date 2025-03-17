from airflow import DAG
from airflow.operators.python  import PythonOperator
from airflow.hooks.base import BaseHook
import pyodbc
from datetime import datetime

def connect_to_db():
    # Fetch the connection details from Airflow
    conn = BaseHook.get_connection('sqlserver_alper')
    
    # Construct the connection string
    conn_str = (
        f"DRIVER={conn.extra_dejson.get('driver', 'ODBC Driver 17 for SQL Server')};"
        f"SERVER={'LAPTOP-LLBKM3AD\\SQLEXPRESS'};"
        f"DATABASE={'EDWH'}';"
        f"UID={conn.login};"
        f"PWD={conn.password}"
    )
    
    # Connect to the database
    connection = pyodbc.connect(conn_str,timeout=30)
    
    # Example query
    cursor = connection.cursor()
    cursor.execute("SELECT @@version;")
    row = cursor.fetchone()
    print(row)
    
    # Close the connection
    cursor.close()
    connection.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 15),
    'retries': 100,
}

dag = DAG(
    'example_pyodbc_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define the task
connect_task = PythonOperator(
    task_id='connect_to_db_task',
    python_callable=connect_to_db,
    dag=dag,
)
