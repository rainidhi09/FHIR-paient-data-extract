from paient_data_process import *

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master('local')\
        .appName("fhir-paient-data") \
        .config("spark.driver.extraClassPath",
            "/Users/vishalrai/Downloads/mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar")\
        .getOrCreate()
    path = "/Users/vishalrai/Documents/learning/FHIR-paient-data-extract/data/data.json"

    name_df, address_df = execute_source_operation(spark, path)
    execute_sink_operation(name_df, address_df)