#from app_utils.utils import LOG_FILE_NAME, setup_logger
import spark.ufo as ufo
from spark.spark_utils import spark_session, load_source_data
from pyspark.sql import SparkSession


#logger = setup_logger("pipeline", LOG_FILE_NAME)

def create_delta_tables(spark: SparkSession) -> None:
    """Creates bronze tables for initial data extraction"""

    ufo.ufo_bronze_table(spark, "ufo_bronze")
    ufo.ufo_silver_table(spark, "ufo_silver")

if __name__ == "__main__":
    spark = spark_session()
    create_delta_tables(spark)
    
    #create_delta_tables(spark)
    #df_ufo = ufo.scrape_ufo_data("https://nuforc.org/webreports/ndxevent.html")
   # s.load_source_data(spark, df_ufo, "./spark-warehouse/ufo/bronze")
   # print(spark.read.format("delta").load("./spark-warehouse/ufo/bronze").count())

    df_ufo_silver = ufo.ufo_silver_transform(spark).limit(5)
    load_source_data(spark, df_ufo_silver, "./lakehouse/ufo/silver")
    print(spark.read.format("delta").load("./lakehouse/ufo/silver").show())
