#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


"""
### Unravel data - Test project - Model data pipeline
This DAG is demonstrating to update airflow metadata on indexed ES docs.
"""

import json
import itertools
import requests

import pendulum
from pathlib import Path
from elasticsearch import Elasticsearch
from airflow import DAG

from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor



with DAG(
    dag_id='unravel_data_dag_finish',
    description='Index Data DAG',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2022, 8, 15, tz="UTC"),
    catchup=False,
) as dag:
   
    def myfunc(dt, context):
        print(context)
        return dt + timedelta(0)

    sensor = ExternalTaskSensor(
        task_id='index_data_after_sensing',
        external_dag_id='unravel_data_dag',
        external_task_id='load',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=myfunc,
    )
    def index(**kwargs):

        # code to update the indexed document
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
        pass

    index_task = PythonOperator(
        task_id='index',
        python_callable=index,
        provide_context=True,
    )
  
    sensor >> index_task

