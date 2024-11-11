from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import timezone, timedelta, datetime
import sys

sys.path.insert(0, "..")
from disaster import XML_saver


def main():
    try:
        config_path = "../disaster.json"
        xml_saver = XML_saver(config_path)
        xml_saver.save_xml()

    except Exception as e:
        raise AirflowFailException(e)


def create_dag(start_date, schedule_interval, name, python_callable):
    tags = ["disaster"]

    y, mon, d, h, m, s = start_date

    default_args = {
        "owner": "airflow",
        "start_date": datetime(y, mon, d, h, m, s, tzinfo=timezone(timedelta(hours=8))),
        "retry_delay": timedelta(minutes=1),
        "catchup": False,
        "on_failure_callback": slack_notification,
    }

    with DAG(
        name,
        default_args=default_args,
        schedule_interval=schedule_interval,
        max_active_runs=1,
        tags=tags,
    ) as dag:

        task = PythonOperator(
            task_id=name,
            python_callable=python_callable,
            dag=dag,
        )

    return dag


dag = create_dag(
    start_date=(2024, 11, 11, 0, 0, 0),
    schedule_interval="0 * * * *",
    name="disaster",
    python_callable=main,
)
