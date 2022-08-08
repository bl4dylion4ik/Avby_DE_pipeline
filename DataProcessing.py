from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime


YC_INPUT_DATA_BUCKET = ''
YC_OUTPUT_DATA_BUCKET = ''
DATE = datetime.now().strftime('%Y-%m-%d')

auto_detail_columns = ['brand', 'model', 'generation',
                       'year', 'engine_capacity',
                       'engine_type', 'transmission_type',
                       'body_type', 'drive_type', 'color',
                       'mileage_km', 'condition']


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False


spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df = spark.read.format('json') \
    .load(f's3a://{YC_INPUT_DATA_BUCKET}/{DATE}/*.json')


df_fact = df.select(col('id'),
                    col('price.usd.currency'),
                    col('price.usd.amount').cast(DoubleType()).alias('amount '),
                   col('publishedAt').cast(TimestampType()),
                   col('locationName'),
                   col('sellerName'),
                   col('indexPromo'),
                   col('top'),
                   col('highlight'),
                   col('status'),
                   col('publicUrl'))

df_auto = df.select(explode(df.properties)\
                    .alias("dict_id")).select(col("dict_id.name"),
                    col("dict_id.value")).toPandas().set_index('name').T
df_auto = spark.createDataFrame(df_auto)

for detail in auto_detail_columns:
    if has_column(df_auto, detail) is False:
        df_auto = df_auto.withColumn(detail, lit(None))
    else:
        continue

df_auto = df_auto.select(*auto_detail_columns)
df_final = df_fact.join(df_auto)
df_final.write.format('csv') \
    .save(f's3a://{YC_OUTPUT_DATA_BUCKET}/{DATE}/')