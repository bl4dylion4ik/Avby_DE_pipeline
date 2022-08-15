import logging
from datetime import datetime
import boto3
import json
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocCreatePysparkJobOperator
)
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
import requests

from scraper import BRAND_LIST, get_pages, get_id_list_per_page, get_info_by_id, get_count_of_brand

YC_DP_FOLDER_ID = 'b1gugjqmcqtrm4l25tat'
YC_DP_CLUSTER_NAME = f'tmp-dp-{uuid.uuid4()}'
YC_DP_CLUSTER_DESC = "Temporary cluster for Spark processing under Air flow orchestration"
YC_DP_SUBNET_ID = 'e9b6hebtksbtd1o1tj7g'  # YC subnet to create cluster
YC_DP_SA_ID = 'aje104fk12454el9el5u'  # YC service account for Data Proc cluster

YC_INPUT_DATA_BUCKET = 'av-input'  # YC S3 bucket for input data
YC_SOURCE_BUCKET = "av-processing"  # YC S3 bucket for pyspark source files
YC_DP_LOGS_BUCKET = "av-logs"  # YC S3 bucket for Data Proc cluster logs


def extract_from_api(brand_id: int, templates_dict: dict, **context):
    list_info = []
    for page in range(int(templates_dict['count_pages'])):
        for _id in get_id_list_per_page(brand_id, page):
            try:
                info = get_info_by_id(int(_id))
                list_info.append(info)
            except requests.exceptions.ConnectionError as e:
                logging.warning(f'ConnectionError, more detail {e}')
                continue
    serialized_info = json.dumps(list_info)
    context["task_instance"].xcom_push(key="info", value=serialized_info)


def load_to_cloud(brand_name: str, date: str, templates_dict: dict, **context):
    aws_access_key_id = Variable.get("aws_access_key_id")
    aws_secret_access_key = Variable.get("aws_secret_access_key")
    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    infos = json.loads(templates_dict['info'])
    logging.info(f'type list {type(infos)}')
    for info in infos:
        serialized_info = json.dumps(info)
        s3.put_object(Bucket=f'av-input', Key=f'{date}/{brand_name}/{info["id"]}.json', Body=serialized_info)


def generate_dag(brand_name: str, brand_id: int, number: int):
    schedule_interval = f'{number%60} {10+number//60} * * *'
    with DAG(
            dag_id=f'{brand_id}_to_cloud',
            schedule_interval=schedule_interval,
            start_date=datetime(2022, 8, 13)
    ) as dag:

        count_number = PythonOperator(task_id="count_number",
                                      python_callable=get_count_of_brand,
                                      op_kwargs={"brand_id": brand_id},
                                      dag=dag)

        count_pages = PythonOperator(task_id="count_pages",
                                     python_callable=get_pages,
                                     templates_dict={
                                         "count": "{{task_instance.xcom_pull(task_ids='count_number', key='count')}}"
                                     },
                                     dag=dag)

        extract = PythonOperator(task_id="extract_from_api",
                                 python_callable=extract_from_api,
                                 templates_dict={
                                      "count_pages": "{{task_instance.xcom_pull(task_ids='count_pages', key='pages')}}"
                                 },
                                 op_kwargs={"brand_id": brand_id},
                                 dag=dag)

        load_to_storage = PythonOperator(task_id="load_to_storage",
                              python_callable=load_to_cloud,
                              templates_dict={
                                  "info": "{{task_instance.xcom_pull(task_ids='extract_from_api', key='info')}}"
                              },
                              op_kwargs={"brand_name": brand_name, "date": "{{ ds }}"},
                              dag=dag)

        create_spark_cluster = DataprocCreateClusterOperator(
            task_id='dp-cluster-create-task',
            folder_id=YC_DP_FOLDER_ID,
            cluster_name=YC_DP_CLUSTER_NAME,
            cluster_description=YC_DP_CLUSTER_DESC,
            subnet_id=YC_DP_SUBNET_ID,
            s3_bucket=YC_DP_LOGS_BUCKET,
            zone='ru-central1-a',
            service_account_id=YC_DP_SA_ID,
            cluster_image_version='2.0.43',
            masternode_resource_preset='s2.small',
            masternode_disk_size=20,
            masternode_disk_type='network-ssd',
            computenode_resource_preset='m2.large',
            computenode_disk_size=20,
            computenode_disk_type='network-ssd',
            computenode_count=1,
            computenode_max_hosts_count=5,
            services=['YARN', 'SPARK'],
            datanode_count=0,
            dag=dag
        )

        spark_processing = DataprocCreatePysparkJobOperator(
            task_id='dp-cluster-pyspark-task',
            main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/DataProcessing.py',
            args=[brand_name],
            dag=dag
        )

        delete_spark_cluster = DataprocDeleteClusterOperator(
            task_id='dp-cluster-delete-task',
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag
        )

        insert_into_db = ClickHouseOperator(
            task_id='insert_into_clickhouse',
            database='clickhousedb',
            sql=
            f'''
                        INSERT INTO clickhousedb.Auto (
                            id, currency, amount, publishedAt, locationName, indexPromo,
                            top, highlight, status, publicUrl, brand, model, generation, year, engine_capacity,
                            engine_type, transmission_type, body_type, drive_type, color, mileage_km, condition)
                        SELECT id, currency, amount, publishedAt, locationName, indexPromo,
                            top, highlight, status, publicUrl, brand, model, generation, year, engine_capacity,
                            engine_type, transmission_type, body_type, drive_type, color, mileage_km, condition
                        FROM s3('https://storage.yandexcloud.net/av-output/2022-08-15/Daihatsu/*.csv',
                         'YCAJE30XgdytxZagWHrAXw28g', 'YCOJZ_xTx3NdeBpyPrCLw3vcSBY9vi_woPti0pVi', 'CSVWithNames',
                            'id Int64, currency String, amount Float32, publishedAt Date, locationName String,
                            indexPromo bool, top bool, highlight bool, status String, publicUrl String,
                            brand String, model String, generation String, year Int16, engine_capacity Float32,
                            engine_type String, transmission_type String, body_type String, drive_type String,
                            color String, mileage_km Int32, condition String')
                    ''',
            clickhouse_conn_id='clickhouse_connection',
            dag=dag,
        )

        send_email = EmailOperator(task_id="send_email",
                                   to='vert3x.man@gmail.com',
                                   subject='Load process',
                                   html_content=f'{brand_name} is complete. Files load to the storage',
                                   dag=dag)

        count_number >> count_pages >>\
        extract >> load_to_storage >> \
        create_spark_cluster >> spark_processing >> delete_spark_cluster >>\
        insert_into_db >> send_email

        return dag


for i, brand in enumerate(BRAND_LIST):
    globals()[f"{brand['id']}_to_cloud"] = generate_dag(
        brand_name=brand['name'],
        brand_id=int(brand['id']),
        number=i
    )

