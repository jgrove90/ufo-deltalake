from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from app_utils import LOG_FILE_NAME, get_moon_phase, setup_logger
from datetime import datetime
from spark.table_schema import TABLE_PATHS, DELTALAKE
import sys

logger = setup_logger("transformations", LOG_FILE_NAME)

bronze_table = f"./{DELTALAKE}/{TABLE_PATHS.get('bronze')}"
silver_table = f"./{DELTALAKE}/{TABLE_PATHS.get('silver')}"


class Transformation:
    def __init__(self, spark):
        self.spark = spark

    def ufo_silver(self) -> DataFrame:
        """
        Performs transformations for the silver layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed silver layer data.
            If an exception occurs, it returns None.
        """
        try:
            # register the get_moon_phase() function as a UDF
            get_moon_phase_udf = udf(get_moon_phase)

            df = (
                self.spark.read.format("delta")
                .load(bronze_table)
                # filter for USA
                .filter(col("Country") == "USA")
                # change null value to No
                .withColumn(
                    "Images",
                    when(col("Images").isNull(), "No").otherwise(col("Images")),
                )
                # rename the coulmns
                .withColumnRenamed("City", "city")
                .withColumnRenamed("State", "state")
                .withColumnRenamed("Country", "country")
                .withColumnRenamed("Images", "images")
                # add hour 00:00 to the dates missing a time, take note of this in analysis
                .withColumn(
                    "DateTime",
                    when(
                        col("DateTime").rlike("\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}"),
                        col("DateTime"),
                    ).otherwise(concat(col("DateTime"), lit(" 00:00"))),
                )
                # convert date string to date timestamp type
                .withColumn("timestamp", to_timestamp("DateTime", "MM/dd/yy HH:mm"))
                # extract calendar properties
                .withColumn("date", to_date("timestamp"))
                .withColumn("year", year("date"))
                .withColumn("month", month("date"))
                .withColumn("dayOfWeek", date_format("date", "EEEE"))
                .withColumn("week", weekofyear("date"))
                .withColumn("hour", hour("timestamp"))
                # select dates that have correct format
                .where(
                    (
                        (length(trim(col("DateTime"))) > 6)
                        | (length(trim(col("DateTime"))) < 13)
                    )
                )
                .where(length(col("year")) == 4)
                .where(col("year") <= datetime.today().year)
                # call the moon_phase_udf
                .withColumn("moonPhaseAngle", get_moon_phase_udf("date").cast("double"))
                # drop columns/rows
                .drop(col("DateTime"), "Posted", "timestamp", "temp_address")
                .dropDuplicates()
                .dropna()
            )
            logger.info(f"Silver layer transformation completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()

    def ufo_gold_location(self) -> DataFrame:
        """
        Performs transformations for the dim_location table in the gold layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed dim_location data.
            If an exception occurs, it returns None.
        """
        try:
            df = (
                self.spark.read.format("delta")
                .load(silver_table)
                .select(
                    "city",
                    "state",
                    "country",
                )
                .dropDuplicates()
            )
            logger.info(f"Gold layer transformation for dim_location completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()

    def ufo_gold_description(self) -> DataFrame:
        """
        Performs transformations for the dim_description table in the gold layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed dim_description data.
            If an exception occurs, it returns None.
        """
        try:
            df = (
                self.spark.read.format("delta")
                .load(silver_table)
                .select(
                    "shape",
                    "duration",
                    "summary",
                    "images",
                )
                .dropDuplicates()
            )
            logger.info(f"Gold layer transformation for dim_description completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()

    def ufo_gold_date(self) -> DataFrame:
        """
        Performs transformations for the dim_transform table in the gold layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed dim_transform data.
            If an exception occurs, it returns None.
        """
        try:
            df = (
                self.spark.read.format("delta")
                .load(silver_table)
                .select(
                    "date",
                    "year",
                    "month",
                    "dayOfWeek",
                    "week",
                    "hour",
                )
                .dropDuplicates()
            )
            logger.info(f"Gold layer transformation for dim_date completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()

    def ufo_gold_astro(self) -> DataFrame:
        """
        Performs transformations for the dim_astro_transform table in the gold layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed dim_astro_transform data.
            If an exception occurs, it returns None.
        """
        try:
            df = (
                self.spark.read.format("delta")
                .load(silver_table)
                .select(
                    "moonPhaseAngle",
                )
                .dropDuplicates()
            )
            logger.info(f"Gold layer transformation for dim_astro completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()

    def ufo_gold_fact(self) -> DataFrame:
        """
        Performs transformations for the fact_transform table in the gold layer.

        Args:
            spark (SparkSession): The SparkSession object for interacting with Spark.

        Returns:
            DataFrame: A DataFrame containing the transformed fact_transform data.
            If an exception occurs, it returns None.
        """
        try:
            df = (
                self.spark.read.format("delta")
                .load(silver_table)
                .select(
                    "state",
                    "city",
                    "shape",
                    "year",
                    "month",
                    "week",
                    "dayofweek",
                    "hour",
                    "moonPhaseAngle",
                )
                .groupBy(
                    "state",
                    "city",
                    "shape",
                    "year",
                    "month",
                    "week",
                    "dayofweek",
                    "hour",
                    "moonPhaseAngle",
                )
                .agg(
                    count(col("state")).cast("int").alias("state_count"),
                    count(col("city")).cast("int").alias("city_count"),
                    count(col("shape")).cast("int").alias("shape_count"),
                    count(col("year")).cast("int").alias("year_count"),
                    count(col("month")).cast("int").alias("month_count"),
                    count(col("week")).cast("int").alias("week_count"),
                    count(col("dayofweek")).cast("int").alias("day_count"),
                    count(col("hour")).cast("int").alias("hour_count"),
                    count(col("moonPhaseAngle")).cast("int").alias("phaseangle_count"),
                )
            )
            logger.info(f"Gold layer transformation for fact completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
            sys.exit()
