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
This DAG is demonstrating an Extract Hive -> Transform -> Load data into Elasticsearch
"""

import json
from textwrap import dedent
import ijson
import itertools
import decimal

import pendulum
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from airflow import DAG

from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

with DAG(
    dag_id='unravel_data_dag',
    description='Unravel Data DAG',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2022, 8, 15, tz="UTC"),
    catchup=False,
) as dag:
    file_path = Path.cwd()/'airflow'/'example_dags'/'source'/'hive-apps.json'
    # [START extract_function]
    def extract(file_path,**kwargs):
        # Retrieve fixed record of data from the json array by parsing it. Index of data is stored in Airflow Variables.
        next_index_to_parse = int(Variable.get("unravel_data"))
        extracted_data = []
        with open(file_path, 'rb') as input_file:
            jsonobj = ijson.items(input_file, 'item')
            jsons = (o for o in jsonobj)
            my_json=itertools.islice(jsons,next_index_to_parse, next_index_to_parse+10)
            for data in my_json:
                extracted_data.append(json.dumps(data, cls=DecimalEncoder))
        ti = kwargs['ti']
        ti.xcom_push('extracted_data', extracted_data)

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        # Do all the transformation of the data in this.
        ti = kwargs['ti']
        extract_data_values = ti.xcom_pull(task_ids='extract', key='extracted_data')
        transformed_data = []
        for each_data in extract_data_values:
            modified_data = {}
            extracted_data = json.loads(each_data)
            modified_data['id'] = extracted_data['id']
            modified_data['vcoreSeconds'] = extracted_data['vcoreSeconds']
            modified_data['memorySeconds'] = extracted_data['memorySeconds']
            modified_data['IO'] = extracted_data['totalDfsBytesRead']+ extracted_data['totalDfsBytesWritten']
            modified_data['duration'] = (extracted_data['duration']/(1000*60))%60
            transformed_data.append(json.dumps(modified_data))
        ti.xcom_push('transformed_data', transformed_data)

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        # Load the data into ES with index and update the variable
        ti = kwargs['ti']
        update_info = {'dag_name': kwargs['dag'].dag_id, 'run_date': kwargs['ts']}
        transformed_data_values = ti.xcom_pull(task_ids='transform', key='transformed_data')
        es_endpoint = Variable.get('ES_ENDPOINT')
        es = Elasticsearch(es_endpoint, verify_certs=False)
        es.indices.create(index='app-summary', ignore=400)
        actions = [
            {
                "_index": "app-summary",
                "_source": {**json.loads(data), **update_info}
            } for data in transformed_data_values
        ]
        bulk(es, actions)
        old_index = int(Variable.get("unravel_data"))
        my_var = Variable.set("unravel_data", old_index+10)

    # [END load_function]

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_args=[file_path],
    )
    

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
  
    extract_task >> transform_task >> load_task

