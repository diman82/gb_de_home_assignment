#!/usr/bin/env python3

# MWAA: Trigger an Apache Airflow DAG using SDK

import logging
from airflow.api.client.local_client import Client

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)


def main():
    c = Client(None, None)
    c.trigger_dag(dag_id='spark_pi_example', run_id='spark_pi_example', conf={})


if __name__ == '__main__':
    main()
