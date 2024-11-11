from airflow.exceptions import AirflowFailException
from disaster_dag import create_dag

import sys

sys.path.insert(0, "..")
from VPCK50 import ETL_VPCK50


def main():
    try:
        config_path = "../disaster.json"
        etl_vpck50 = ETL_VPCK50(config_path, "regular", "VPCK50")
        etl_vpck50.xml_to_csv()

    except Exception as e:
        raise AirflowFailException(e)


dag = create_dag(
    start_date=(2024, 11, 11, 0, 0, 0),
    schedule_interval="0 * * * *",
    name="disaster_VPCK50",
    python_callable=main,
)
