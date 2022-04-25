from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master('local')\
        .appName("fhir-paient-data") \
        .getOrCreate()

    df = spark.read \
        .option("multiLine", True) \
        .json("/Users/vishalrai/Documents/learning/FHIR-paient-data-extract/data/data.json") \
        .withColumn("id", monotonically_increasing_id())
    df.show(1, False)

    df.select(col('address').getItem(0)['city']).show(1, False)
    df.printSchema()


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

    convertUDF = udf(lambda z: convert_name(z), MapType(StringType(), StringType()))

    df2 = df.select(col('birthdate'), col('gender'), convertUDF(col("name")).alias("newName")) \
        .withColumn('maiden_name', col('newName').getItem('maiden_name')) \
        .withColumn('usual_name', col('newName').getItem('usual_name')) \
        .withColumn('official_name', col('newName').getItem('official_name')).show(truncate=False)

    df3 = df.select(explode('address').alias('new_address')) \
        .withColumn('use', col('new_address').use) \
        .withColumn('type', col('new_address').type) \
        .withColumn('city', col('new_address').city) \
        .withColumn('district', col('new_address').district) \
        .withColumn('state', col('new_address').state).withColumn('postalCode', col('new_address').postalCode)
    df3.show()