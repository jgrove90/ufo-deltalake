from pyspark.sql import SparkSession
from app_utils import LOG_FILE_NAME, get_package_version, setup_logger, get_file_path


logger = setup_logger("spark_utils", LOG_FILE_NAME)


class SparkSessionCreator:
    def __init__(self):
        self.spark = None

    def create_spark_session(self):
        """
        Creates a SparkSession object for interacting with Spark.

        Returns:
            SparkSession: The SparkSession object.

        Note:
            The function assumes that the necessary files for distribution
            are located under ./src
        """
        try:
            delta_version = get_package_version("delta-spark")

            self.spark = SparkSession.builder.getOrCreate()

            # Distributes files across worker nodes
            [self.spark.sparkContext.addPyFile(file) for file in get_file_path("./src", ".py")]

            self.spark.sparkContext.setLogLevel("ERROR")

            logger.info(
                f"Created SparkSession for app:{self.spark.conf.get('spark.app.name')} @ {self.spark.sparkContext.master} using delta-spark:{delta_version}"
            )

            return self.spark
        except Exception as e:
            logger.error(f"{e}")
