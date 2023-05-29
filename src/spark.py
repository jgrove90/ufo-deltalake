from pyspark.sql import SparkSession
from utils import setup_logger, LOG_FILE_NAME, get_package_version
from importlib.metadata import version

DATA_LAKE_PATH = "./data_lake"

logger = setup_logger("spark_etl", LOG_FILE_NAME)


def spark_session(app_name: str, master: str) -> SparkSession:
    """Creates a SparkSession"""
    try:
        delta_version = get_package_version("delta-spark")

        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.jars.packages", f"io.delta:delta-core_2.12:{delta_version}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

        logger.info(
            f"Created SparkSession for app:{app_name} @ {master} using delta-spark:{delta_version}"
        )

        return spark
    except Exception as e:
        logger.error(f"{e}")


# FIXME: edit this extract method to
def extract_from_source(spark_session: SparkSession, folder: str):
    """Extracts data from source"""
    try:
        df = spark_session.range(0, 10)

        df.write.format("delta").mode("overwrite").save(f"{DATA_LAKE_PATH}/{folder}")

        logger.info(f"Extracted successfully from ________.")
    except Exception as e:
        logger.error(f"{e}")


extract_from_source(spark_session("ufo", "local[*]"), "ufo_bronze")
