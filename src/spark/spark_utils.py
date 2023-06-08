from pyspark.sql import SparkSession, DataFrame
from delta import DeltaTable
from app_utils import LOG_FILE_NAME, get_package_version, setup_logger


logger = setup_logger("spark_utils", LOG_FILE_NAME)


def spark_session() -> SparkSession:
    """Creates a SparkSession"""
    try:
        delta_version = get_package_version("delta-spark")

        spark = SparkSession.builder.getOrCreate()

        # Distributes files across worker nodes
        file_paths = ["./src/app_utils.py", "./src/address_cleaning.py"]
        [spark.sparkContext.addPyFile(file) for file in file_paths]

        logger.info(
            f"Created SparkSession for app:{spark.conf.get('spark.app.name')} @ {spark.sparkContext.master} using delta-spark:{delta_version}"
        )

        return spark
    except Exception as e:
        logger.error(f"{e}")


def optimize_table(spark: SparkSession, table_path: str) -> DataFrame:
    """Coalesces small files into larger ones"""
    try:
        deltatable = (
            DeltaTable.forPath(spark, table_path).optimize().executeCompaction()
        )
        logger.info(f"Coalescing files @ {table_path}")
        return deltatable
    except Exception as e:
        logger.info(f"{e}")


def delta_vacuum(spark: SparkSession, table_path: str, retention: int) -> DataFrame:
    """Removes old files based on retention period"""
    try:
        deltaTable = DeltaTable.forPath(spark, table_path)
        logger.info(f"Vacuuming data after optimization @ {table_path}")
        return deltaTable.vacuum(retention)
    except Exception as e:
        logger.error(f"{e}")


def load_source_data(spark: SparkSession, df: DataFrame, table_path: str) -> DataFrame:
    """Loads source data into delta table"""
    try:
        df.write.format("delta").mode("overwrite").save(table_path)
        logger.info(f"Data successfully loaded into delta lake @ {table_path}")
        optimize_table(spark, table_path)
        return delta_vacuum(spark, table_path, 0)
    except Exception as e:
        logger.error(f"{e}")
