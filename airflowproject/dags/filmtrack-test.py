from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

ctx = snowflake.connector.connect(
        user='joaquinpizarro',
        password='Distillery21',
        account='eea26754.us-east-1',
        warehouse='COMPUTE_WH',
        schema='PUBLIC',
        role='ACCOUNTADMIN',
        database='DEMO_DB'
    )

def _create_table():
    cs = ctx.cursor()
    try:
        cs.execute("CREATE TABLE IF NOT EXISTS DEMO_DB.PUBLIC.FILMTRACK (filmtrack_data varchar(100), dia date) ")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()

def _insert_snowflake():
    cs = ctx.cursor()
    try:
        cs.execute("INSERT INTO FILMTRACK VALUES('SNOWFLAKE','2021-07-17 10:57:21.000')")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()


with DAG(dag_id='filmtrack-test', default_args=default_args, schedule_interval="*/30 * * * *", start_date=datetime(2021, 1, 1)) as dag:

    crate_table = PythonOperator(
        task_id='create_snowflake_table',
        python_callable=_create_table
    )

    insert_snowflake = PythonOperator(
        task_id='insert_snowflake_table',
        python_callable = _insert_snowflake
    )

    crate_table.set_downstream(insert_snowflake)
