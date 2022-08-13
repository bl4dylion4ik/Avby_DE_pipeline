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
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
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


# def load_to_storage(brand_name: str, brand_id: int, date: str, templates_dict: dict, **context):
#     aws_access_key_id = Variable.get("aws_access_key_id")
#     aws_secret_access_key = Variable.get("aws_secret_access_key")
#     session = boto3.session.Session()
#     list_info = []
#     s3 = session.client(service_name='s3',
#                         endpoint_url='https://storage.yandexcloud.net',
#                         aws_access_key_id=aws_access_key_id,
#                         aws_secret_access_key=aws_secret_access_key)
#     for page in range(int(templates_dict['count_pages'])):
#         for _id in get_id_list_per_page(brand_id, page):
#             try:
#                 info = get_info_by_id(int(_id))
#                 list_info.append(info)
#             except requests.exceptions.ConnectionError as e:
#                 logging.warning(f'ConnectionError, more detail {e}')
#                 continue
#     serialized_info = json.dumps(list_info).encode('utf-8')
#     s3.put_object(Bucket=f'av-bucket', Key=f'{date}/{brand_name}/{brand_id}.json', Body=serialized_info)


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
    context["task_instance"].xcom_push(key="info", value=list_info)


def load_to_cloud(brand_name: str, date: str, templates_dict: dict, **context):
    aws_access_key_id = Variable.get("aws_access_key_id")
    aws_secret_access_key = Variable.get("aws_secret_access_key")
    session = boto3.session.Session()
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    info = templates_dict['info']
    serialized_info = json.dumps(info).encode('utf-8')
    s3.put_object(Bucket=f'av-input', Key=f'{date}/{brand_name}.json', Body=serialized_info)


def generate_dag(brand_name: str, brand_id: int, number: int):
    schedule_interval = f'{number%60} {10+number//60} * * *'
    with DAG(
            dag_id=f'{brand_id}_to_cloud',
            schedule_interval=schedule_interval,
            start_date=datetime(2022, 8, 13),

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
            task_id='cluster-create',
            folder_id=YC_DP_FOLDER_ID,
            cluster_name=YC_DP_CLUSTER_NAME,
            cluster_description=YC_DP_CLUSTER_DESC,
            subnet_id=YC_DP_SUBNET_ID,
            s3_bucket=YC_DP_LOGS_BUCKET,
            zone='ru-central1-a',
            service_account_id=YC_DP_SA_ID,
            cluster_image_version='2.0.43',
            masternode_resource_preset='s2.small',
            masternode_disk_size=200,
            masternode_disk_type='network-ssd',
            computenode_resource_preset='m2.large',
            computenode_disk_size=200,
            computenode_disk_type='network-ssd',
            computenode_count=2,
            computenode_max_hosts_count=5,
            services=['YARN', 'SPARK'],
            datanode_count=0,
            dag=dag
        )

        spark_processing = DataprocCreatePysparkJobOperator(
            task_id='cluster-pyspark-job',
            main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/DataProcessing.py',
            args=[brand_name],
            dag=dag
        )

        delete_spark_cluster = DataprocDeleteClusterOperator(
            task_id='cluster-delete',
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag
        )

        # connect_to_db = ClickHouseHook()
        #
        # insert_into_db = ClickHouseOperator(
        #     task_id='insert_into_clickhouse',
        #     database='clickhousedb',
        #     sql=
        #     f'''
        #         INSERT INTO clickhousedb.Auto (
        #             Id, brand, model, generation, year, engine_capacity, transmission_type,
        #             body_type, condition, miliage_km, color, drive_type, indexPromo, top, highlighted,
        #             status, publicUrl, SellerName, SellerCity, publishedAt, Currency, Amount)
        #         SELECT Id, brand, model, generation, year, engine_capacity, transmission_type,
        #             body_type, condition, miliage_km, color, drive_type, indexPromo, top, highlighted,
        #             status, publicUrl, SellerName, SellerCity, publishedAt, Currency, Amount
        #         FROM s3('https://storage.yandexcloud.net/av-output/{{ ds }}/{brand_name}.csv',
        #             'CSVWithNames',
        #             'Id Int, brand String, model String, generation String, year Int, engine_capacity Float32,
        #              transmission_type String, body_type String, condition String, miliage_km Double, color String,
        #              drive_type String, indexPromo bool, top bool, highlighted bool, status String, publicUrl String,
        #              SellerName String, SellerCity String, publishedAt DateTime, Currency String, Amount Float32')
        #     ''',
        #     clickhouse_conn_id='clickhouse_connection',
        #     dag=dag,
        # )

        send_email = EmailOperator(task_id="send_email",
                                   to='vert3x.man@gmail.com',
                                   subject='Load process',
                                   html_content=f'{brand_name} is complete. Files load to the storage',
                                   dag=dag)

        count_number >> count_pages >>\
        extract >> load_to_storage >> \
        create_spark_cluster >> spark_processing >> delete_spark_cluster >>\
        send_email

        return dag


for i, brand in enumerate(BRAND_LIST):
    globals()[f"{brand['id']}_to_cloud"] = generate_dag(
        brand_name=brand['name'],
        brand_id=int(brand['id']),
        number=i
    )
