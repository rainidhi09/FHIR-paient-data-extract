from paient_data_process import *

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master('local')\
        .appName("fhir-paient-data") \
        .getOrCreate()

    name_df, address_df = execute_source_operation(spark)
    name_df.show()
    address_df.show()
    execute_sink_operation(name_df, address_df)