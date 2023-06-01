from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from utils import setup_logger, LOG_FILE_NAME, get_package_version


logger = setup_logger("spark_utils", LOG_FILE_NAME)


# TODO: Create test
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
            .config("fs.permissions.umask-mode", "007")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.ui.port", "4050")
            .getOrCreate()
        )

        logger.info(
            f"Created SparkSession for app:{app_name} @ {master} using delta-spark:{delta_version}"
        )

        return spark
    except Exception as e:
        logger.error(f"{e}")


def start_spark_history_server(spark: SparkSession) -> None:
    """Starts spark history server"""
    try: 
        spark.conf.set("spark.eventLog.enabled", "True")
        spark.conf.set("spark.eventLog.dir", "./tmp/spark-logs")
        spark.sparkContext.startHistoryServer()
    except Exception as e:
        logger.error(f"{e}")


def load_source_data(df: DataFrame, table_path: str) -> None:
    """Loads source data into delta table"""
    try:
        df.write.format("delta").mode("overwrite").save(table_path)
        logger.info(f"Data successfully loaded into delta lake @ {table_path}")
    except Exception as e:
        logger.error(f"{e}")
