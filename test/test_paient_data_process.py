from pyspark.sql import *
import src.paient_data_process as f
import pytest
path = "/Users/vishalrai/Documents/learning/FHIR-paient-data-extract/data/data.json"

@pytest.fixture()
def spark():
    return SparkSession.builder\
        .master('local')\
        .appName('first_app')\
        .getOrCreate()


@pytest.fixture()
def expected_data():
    return [('home', 'both', 'PleasantVille', 'Rainbow', 'Vic', '3999'),
            ('home', 'both', 'PleasantVille', 'Rainbow', 'Vic', '3999'),
            ('home', 'both', 'PleasantVille', 'Rainbow', 'Vic', '3999'),
            ('home', 'both', 'PleasantVille', 'Rainbow', 'Vic', '3999')]


def test_df_count(spark):
    name_df, address_df = f.execute_source_operation(spark, path)
    assert (2 == name_df.count())
    assert (4 == address_df.count())
    address_df.show()


def test_using_fixture_df(spark, expected_data):
    expected_df = spark.createDataFrame(expected_data)\
        .toDF("use_add", "type_add", "city", "district", "state", "postalCode")
    name_df, address_df = f.execute_source_operation(spark, path)
    assert (expected_df.collect() == address_df.drop("id").collect())
