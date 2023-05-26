from src.spark import *

def test_spark_builder():
    app_name = "TestApp"

    builder = spark_builder(app_name)

    # check if returned object is the correct type
    assert isinstance(builder, pyspark.sql.session.SparkSession.Builder)

    # check if configs are set correctly
    conf = builder.getOrCreate().conf
    assert conf.get("spark.app.name") == app_name
    assert conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"

def test_spark_session():
    app_name = "TestApp"

    spark = spark_session(app_name)

    # check if the returned object is the correct type
    assert isinstance(spark, pyspark.sql.session.SparkSession)