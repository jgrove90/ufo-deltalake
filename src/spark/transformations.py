from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from app_utils import LOG_FILE_NAME, get_moon_phase, setup_logger
from datetime import datetime
from spark.table_schema import TABLE_PATHS

logger = setup_logger("transformations", LOG_FILE_NAME)


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

            # columns for window functions
            loc_columns = ["city", "state", "country",]
            desc_columns = ["shape", "duration", "summary", "images",]
            date_columns = ["date", "year", "dayOfWeek", "week", "hour",]
        
            # windows
            location_window = Window.orderBy(loc_columns)
            description_window = Window.orderBy(desc_columns)
            date_window = Window.orderBy(date_columns)
            astro_window = Window.orderBy("moonPhaseAngle")
 

            df = (
                self.spark.read.format("delta")
                .load(TABLE_PATHS.get("bronze"))
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
                .drop("DateTime", "Posted", "timestamp", "temp_address")
                .dropDuplicates()
                .dropna()
                # create id columns
                .withColumn(
                    "id", row_number().over(Window.orderBy(asc("date"))).cast("long")
                )
                .withColumn(
                    "id_location", dense_rank().over(location_window).cast("long")
                )
                .withColumn(
                    "id_description", dense_rank().over(description_window).cast("long")
                )
                .withColumn("id_date", dense_rank().over(date_window).cast("long"))
                .withColumn("id_astro", dense_rank().over(astro_window).cast("long"))
                # set the order of columns
                .select(
                    "id",
                    "id_location",
                    "city",
                    "state",
                    "country",
                    "id_description",
                    "shape",
                    "duration",
                    "summary",
                    "images",
                    "id_date",
                    "date",
                    "year",
                    "month",
                    "dayOfWeek",
                    "week",
                    "hour",
                    "id_astro",
                    "moonPhaseAngle",
                )
                .orderBy(desc("id"))
            )
            logger.info(f"Silver layer transformation completed")
            return df
        except Exception as e:
            logger.error(f"{e}")

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
                .load(TABLE_PATHS.get("silver"))
                .select(
                    "id_location",
                    "city",
                    "state",
                    "country",
                )
                .dropDuplicates()
                .orderBy(asc("id_location"))
            )
            logger.info(f"Gold layer transformation for dim_location completed")
            return df
        except Exception as e:
            logger.error(f"{e}")

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
                .load(TABLE_PATHS.get("silver"))
                .select(
                    "id_description",
                    "shape",
                    "duration",
                    "summary",
                    "images",
                )
                .dropDuplicates()
                .orderBy(asc("id_description"))
            )
            logger.info(f"Gold layer transformation for dim_description completed")
            return df
        except Exception as e:
            logger.error(f"{e}")

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
                .load(TABLE_PATHS.get("silver"))
                .select(
                    "id_date",
                    "date",
                    "year",
                    "month",
                    "dayOfWeek",
                    "week",
                    "hour",
                )
                .dropDuplicates()
                .orderBy(asc("id_date"))
            )
            logger.info(f"Gold layer transformation for dim_date completed")
            return df
        except Exception as e:
            logger.error(f"{e}")

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
                .load(TABLE_PATHS.get("silver"))
                .select(
                    "id_astro",
                    "moonPhaseAngle",
                )
                .dropDuplicates()
                .orderBy(asc("id_astro"))
            )
            logger.info(f"Gold layer transformation for dim_astro completed")
            return df
        except Exception as e:
            logger.error(f"{e}")

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
                .load(TABLE_PATHS.get("silver"))
                .select(
                    "id_location",
                    "id_description",
                    "id_date",
                    "id_astro",
                )
            )
            logger.info(f"Gold layer transformation for fact completed")
            return df
        except Exception as e:
            logger.error(f"{e}")
