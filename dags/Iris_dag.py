from __future__ import print_function

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from sklearn import datasets
from sklearn import tree
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import psycopg2


def iris_predictions(**kwargs):
    iris = datasets.load_iris()
    x = iris.data
    y = iris.target
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.5)
    classifier = tree.DecisionTreeClassifier()
    classifier.fit(x_train, y_train)
    predictions = classifier.predict(x_test)
    acc = accuracy_score(y_test, predictions)
    return {"accuracy": acc}


def upload_result_to_db(**kwargs):
    ti = kwargs['ti']
    acc = ti.xcom_pull(task_ids='iris_predictions')
    print(acc)
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="airflow",
        password="airflow")
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS IRIS_ACCURACY (
                  accuracy varchar(45) NOT NULL,
                  date_added timestamp NOT NULL,
                  PRIMARY KEY (date_added)
                )""")
    cursor.execute(
        f"""INSERT INTO IRIS_ACCURACY (accuracy,date_added) 
        VALUES ({acc.get("accuracy")}, current_timestamp);""")
    conn.commit()
    cursor.close()
    conn.close()


args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 10),
    'schedule_interval': '@daily',
}

dag = DAG(
    dag_id='iris', default_args=args)

load_data = PythonOperator(
    task_id='iris_predictions',
    python_callable=iris_predictions,
    provide_context=True,
    dag=dag)

results = PythonOperator(
    task_id='upload_to_database',
    python_callable=upload_result_to_db,
    provide_context=True,
    dag=dag)

load_data >> results
