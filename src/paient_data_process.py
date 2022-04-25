from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType


def convert_name(array_str):
    name = {}
    for item in array_str:
        if item['use'] == 'usual':
            name['usual_name'] = item['family'] + " " + item['given'][0] + " " + item['given'][1] if len(
                item['given']) > 1 else \
                item['given'][0]
        elif item['use'] == 'official':
            name['official_name'] = item['family'] + " " + item['given'][0] + " " + item['given'][1] if len(
                item['given']) > 1 else \
                item['given'][0]
        elif item['use'] == 'maiden':
            name['maiden_name'] = item['family'] + " " + item['given'][0] + " " + item['given'][1] if len(
                item['given']) > 1 else \
                item['given'][0]
    return name


def read_data(spark):
    return spark.read \
        .option("multiLine", True) \
        .json("/Users/vishalrai/Documents/learning/FHIR-paient-data-extract/data/data.json") \
        .withColumn("id", monotonically_increasing_id())


def transform_name(df):

    convertUDF = udf(lambda z: convert_name(z), MapType(StringType(), StringType()))

    return df.select(col('birthdate'), col('gender'), convertUDF(col("name")).alias("newName")) \
        .withColumn('maiden_name', col('newName').getItem('maiden_name')) \
        .withColumn('usual_name', col('newName').getItem('usual_name')) \
        .withColumn('official_name', col('newName').getItem('official_name'))


def transform_address(df):
    return df.select(explode('address').alias('new_address')) \
        .withColumn('use', col('new_address').use) \
        .withColumn('type', col('new_address').type) \
        .withColumn('city', col('new_address').city) \
        .withColumn('district', col('new_address').district) \
        .withColumn('state', col('new_address').state).withColumn('postalCode', col('new_address').postalCode)


def execute_source_operation(spark):
    df = read_data(spark)
    name_df = transform_name(df)
    address_df = transform_address(df)
    return name_df, address_df


def execute_sink_operation(name_df, address_df):
    pass
