import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import boto3
import json
import requests

from scraper import BRAND_LIST, get_pages, get_id_list_per_page, get_info_by_id, get_count_of_brand


def load_to_storage(brand_name: str, brand_id: int, date: str, templates_dict: dict, **context):
    aws_access_key_id = Variable.get("aws_access_key_id")
    aws_secret_access_key = Variable.get("aws_secret_access_key")
    session = boto3.session.Session()
    list_info = []
    s3 = session.client(service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    for page in range(int(templates_dict['count_pages'])):
        for _id in get_id_list_per_page(brand_id, page):
            try:
                info = get_info_by_id(int(_id))
                list_info.append(info)
            except requests.exceptions.ConnectionError as e:
                logging.warning(f'ConnectionError, more detail {e}')
                continue
    serialized_info = json.dumps(list_info).encode('utf-8')
    s3.put_object(Bucket=f'av-bucket', Key=f'{date}/{brand_name}/{brand_id}.json', Body=serialized_info)


def generate_dag(brand_name: str, brand_id: int, number: int):
    schedule_interval = f'{number%60} {9+number//60} * * *'
    with DAG(
            dag_id=f'{brand_id}_to_cloud',
            schedule_interval=schedule_interval,
            start_date=datetime(2022, 8, 6),

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

        load = PythonOperator(task_id="load_to_storage",
                              python_callable=load_to_storage,
                              templates_dict={
                                  "count_pages": "{{task_instance.xcom_pull(task_ids='count_pages', key='pages')}}"
                              },
                              op_kwargs={"brand_id": brand_id, "brand_name": brand_name, "date": "{{ ds }}"},
                              dag=dag)

        send_email = EmailOperator(task_id="send_email",
                                   to='vert3x.man@gmail.com',
                                   subject='Load process',
                                   html_content=f'{brand_name} is complete. Files load to the storage',
                                   dag=dag)

        count_number >> count_pages >> load >> send_email

        return dag


for i, brand in enumerate(BRAND_LIST):
    globals()[f"{brand['id']}_to_cloud"] = generate_dag(
        brand_name=brand['name'],
        brand_id=int(brand['id']),
        number=i
    )
