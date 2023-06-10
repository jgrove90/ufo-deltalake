from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from app_utils import LOG_FILE_NAME, get_moon_phase, setup_logger

logger = setup_logger("transformations", LOG_FILE_NAME)


def ufo_silver_transform(spark: SparkSession) -> DataFrame:
    """Performs transformations for the silver layer"""
    try:
        # register the get_moon_phase() function as a UDF
        get_moon_phase_udf = udf(get_moon_phase)

        df = spark.read.format("delta").load("./lakehouse/ufo/bronze")

        df = (
            # filter for USA
            df.filter(df.Country == "USA")
            # change null value to No
            .withColumn("Images", when(df.Images.isNull(), "No").otherwise(df.Images))
            # rename the coulmns
            .withColumnRenamed("City", "city")
            .withColumnRenamed("State", "state")
            .withColumnRenamed("Country", "country")
            .withColumnRenamed("Images", "images")
            # add hour 00:00 to the dates missing a time
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
            .where(((length(trim(df.DateTime)) > 6) | (length(trim(df.DateTime)) < 13)))
            .where(length(col("year")) == 4)
            # call the moon_phase_udf
            .withColumn("moonPhaseAngle", get_moon_phase_udf("date").cast("double"))
            # drop columns/rows
            .drop("DateTime", "Posted", "timestamp", "temp_address")
            .dropDuplicates()
            .dropna()
            # create id column
            .withColumn(
                "id", row_number().over(Window.orderBy(asc("date"))).cast("long")
            )
            # set the order of columns
            .select(
                "id",
                "city",
                "state",
                "country",
                "shape",
                "duration",
                "summary",
                "images",
                "date",
                "year",
                "month",
                "dayOfWeek",
                "week",
                "hour",
                "moonPhaseAngle",
            )
        )
        logger.info(f"Silver layer transformation completed")
        return df
    except Exception as e:
        logger.error(f"{e}")
