from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


def convert_name(array_str):
    name = {}
    for item in array_str:
        if item['use'] == 'usual':
            name['usual_name'] = item.asDict(True)
        elif item['use'] == 'official':
            name['official_name'] = item.asDict(True)
        elif item['use'] == 'maiden':
            name['maiden_name'] = item.asDict(True)
    return name


def read_data(spark, path):
    return spark.read \
        .option("multiLine", True) \
        .json(path) \
        .withColumn("id", monotonically_increasing_id())


def transform_name(df):
    """getting all different types of  names and other information from the row data.  """
    schema = StructType([
        StructField('family', StringType(), True),
        StructField('given', ArrayType(StringType(), True)),
        StructField('period', StructType([StructField('end', StringType())]), True),
        StructField('use', StringType(), True)
    ])

    convertUDF = udf(lambda z: convert_name(z), MapType(StringType(), schema))

    return df.select(col('id'), col('birthdate'), col('gender'), convertUDF(col("name")).alias("newName"))\
        .withColumn('maiden_name', to_json(col('newName').getItem('maiden_name'))) \
        .withColumn('usual_name', to_json(col('newName').getItem('usual_name'))) \
        .withColumn('official_name', to_json(col('newName').getItem('official_name'))).drop("newName")


def transform_address(df):
    """getting multiple address details from nested value(row data) and putting them into different rows. """
    return df.select(col('id'), explode('address').alias('new_address')) \
        .withColumn('use_add', col('new_address').use) \
        .withColumn('type_add', col('new_address').type) \
        .withColumn('city', col('new_address').city) \
        .withColumn('district', col('new_address').district) \
        .withColumn('state', col('new_address').state)\
        .withColumn('postalCode', col('new_address').postalCode)\
        .drop('new_address')


def execute_source_operation(spark, path):
    df = read_data(spark, path)
    name_df = transform_name(df)
    address_df = transform_address(df)
    return name_df, address_df


def execute_sink_operation(name_df, address_df):
    """sink data in multiple tables of mysql."""
    name_df.write.format('jdbc').options(
        url='jdbc:mysql://localhost/sql_store',
        driver='com.mysql.jdbc.Driver',
        dbtable='fhir_patient_detail',
        user='root',
        password='test').mode('append').save()

    address_df.write.format('jdbc').options(
        url='jdbc:mysql://localhost/sql_store',
        driver='com.mysql.jdbc.Driver',
        dbtable='fhir_patient_address_detail',
        user='root',
        password='test').mode('append').save()
