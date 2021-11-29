from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta,date
from pymongo import MongoClient

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

connection_string = "mongodb://172.21.0.2:27017"
client = MongoClient(connection_string)
db = client.get_database("testdb")
collections = db.get_collection("employees")

def _connect_mongo():
    print(collections)

def _insert_data():
    documents = []
    documents.append({"item": "joaquin",
                      "qty": 100,
                      "tags": ["cotton"],
                      "size": {"h": 28, "w": 35.5, "uom": "cm"}
                      })
    documents.append({"item": "joaquin2",
                      "qty": 90,
                      "tags": ["tag1"],
                      "size": {"h": 90, "w": 90.5, "uom": "l"}
                      })
    documents.append({"item": "joaquin3",
                      "qty": 5553,
                      "tags": ["cotton", "rusty", "ryban"],
                      "size": {"h": 1, "w": 2.2, "uom": "cm2"}
                      })
    response = collections.insert_many(documents)
    last_inserted_id = response.inserted_ids
    count = len(documents)
    print("Last inserted Id: {}".format(last_inserted_id))
    return count

def _count_insert_mongo(ti):
    my_xcom = ti.xcom_pull(key='return_value', task_ids=['insert_data'])
    print(my_xcom)

def _extract_from_mongo():
    selected_rows = [doc for doc in collections.find()]
    print(selected_rows)


with DAG(dag_id='etl_mongodb', default_args=default_args, schedule_interval="*/30 * * * *", start_date=datetime(2021, 1, 1)) as dag:

    connect_mongo = PythonOperator(
        task_id = 'connect_mongo',
        python_callable = _connect_mongo)

    insert_data = PythonOperator(
        task_id = 'insert_data',
        python_callable = _insert_data
    )

    count_insert_mongo = PythonOperator(
        task_id = 'count_insert_mongo',
        python_callable = _count_insert_mongo
    )

    extract_from_mongo = PythonOperator(
        task_id = 'extract_from_mongo',
        python_callable = _extract_from_mongo
    )

    connect_mongo.set_downstream(insert_data)
    insert_data.set_downstream(count_insert_mongo)
    count_insert_mongo.set_downstream(extract_from_mongo)