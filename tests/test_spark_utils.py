import pytest
from app_utils import app_utils
from pyspark.sql import SparkSession, DataFrame
from delta import DeltaTable


@pytest.fixture
def spark_session():
    """Creates a SparkSession."""
    app_name = "test_spark_session"
    master = "local"

    delta_version = app_utils.get_package_version("delta-spark")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", f"io.delta:delta-core_2.12:{delta_version}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("fs.permissions.umask-mode", "007")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_spark_session(spark_session):
    """Tests that the SparkSession is created successfully."""
    assert isinstance(spark_session, SparkSession)
    assert spark_session.sparkContext.appName == "test_spark_session"
    assert spark_session.sparkContext.master == "local"

