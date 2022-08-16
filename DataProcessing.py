from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime
import sys


YC_INPUT_DATA_BUCKET = 'av-input'
YC_OUTPUT_DATA_BUCKET = 'av-output'
BRAND_NAME = sys.argv[1]
DATE = datetime.now().strftime('%Y-%m-%d')

auto_detail_columns = ['id', 'brand', 'model', 'generation',
                       'year', 'engine_capacity',
                       'engine_type', 'transmission_type',
                       'body_type', 'drive_type', 'color',
                       'mileage_km', 'condition']


def has_column(df: DataFrame, col: str) -> bool:
    try:
        df[col]
        return True
    except AnalysisException:
        return False


conf = SparkConf().setAppName('Auto')
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

print(f'Read file from s3 bucket: {DATE}/{BRAND_NAME}.json')

df = sql.read.json(f's3a://{YC_INPUT_DATA_BUCKET}/{DATE}/{BRAND_NAME}/*.json')

df_fact = df.select(col('id'),
                    col('price.usd.currency'),
                    col('price.usd.amount').cast(DoubleType()) \
                    .alias('amount'),
                    col('publishedAt').cast(DateType()),
                    col('locationName'),
                    col('indexPromo'),
                    col('top'),
                    col('highlight'),
                    col('status'),
                    col('publicUrl'))

df_preprocees = df.withColumn('dict_id',
                              explode(df.properties)).select(col('id'),
                                                             col("dict_id.name"),
                                                             col("dict_id.value")) \
                                                            .groupBy('id') \
                                                            .pivot('name') \
                                                            .agg(expr("coalesce(first(value), null)"))

for detail in auto_detail_columns:
    if has_column(df_preprocees, detail) is False:
        df_preprocees = df_preprocees.withColumn(detail, lit(None).cast(StringType()))
    else:
        continue

df_preprocees = df_preprocees.select(*auto_detail_columns)
df_final = df_fact.join(df_preprocees, on='id')

print(f'Read file to s3 bucket: {DATE}/{BRAND_NAME}.csv')

df_final.coalesce(1).write.format('csv') \
    .option('header', 'true') \
    .save(f's3a://{YC_OUTPUT_DATA_BUCKET}/{DATE}/{BRAND_NAME}')
