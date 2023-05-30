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
            .config("fs.permissions.umask-mode", "007")
            .getOrCreate()
        )

        logger.info(
            f"Created SparkSession for app:{app_name} @ {master} using delta-spark:{delta_version}"
        )

        return spark
    except Exception as e:
        logger.error(f"{e}")

def ufo_bronze_table(spark_session: SparkSession, table_name: str) -> DeltaTable:
    """Creates a bronze ufo table"""
    try:
        path = "/ufo/bronze"

        if DeltaTable.isDeltaTable(spark_session, f"./spark-warehouse{path}"):
            logger.info(
                f"Delta table: '{table_name}' already exists at './spark-warehouse{path}'"
            )
        else:
            table = (
                DeltaTable.createIfNotExists(spark_session)
                .tableName(table_name)
                .addColumn("timestamp", "STRING")
                .addColumn("city", "STRING")
                .addColumn("state", "STRING")
                .addColumn("country", "STRING")
                .addColumn("shape", "STRING")
                .addColumn("duration", "STRING")
                .addColumn("summary", "STRING")
                .addColumn("posted", "STRING")
                .addColumn("images", "STRING")
                .location(f".{path}")
                .execute()
            )

            logger.info(
                f"Created delta table: {table_name} located at './spark-warehouse{path}'"
            )

            return table
    except Exception as e:
        logger.error(f"{e}")


extract_from_source(spark_session("ufo", "local[*]"), "ufo_bronze")
