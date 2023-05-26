import pyspark
from delta import *
from src.log import setup_logger, LOG_FILE_NAME

DATA_LAKE_PATH = "./data_lake"

logger = setup_logger("spark_etl", LOG_FILE_NAME)

# Create a builder with the Delta extensions
def spark_builder(app_name: str) -> pyspark.sql.session.SparkSession.Builder:
    """Creates a builder with the Delta extensions"""
    try:
        builder = (
            pyspark.sql.SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        logger.info(f"Created spark builder {app_name}")

        return builder
    except Exception as e:
        logger.error(f"{e}")


def spark_session(app_name: str) -> pyspark.sql.session.SparkSession:
    """Creates spark session"""
    try:
        spark = configure_spark_with_delta_pip(spark_builder(app_name)).getOrCreate()

        logger.info(f"Created SparkSession for {app_name}")

        return spark
    except Exception as e:
        logger.error(f"{e}")
