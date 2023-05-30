import spark as s


def create_delta_tables(spark: s.SparkSession) -> None:
    """Creates bronze tables for initial data extraction"""
    s.ufo_bronze_table(spark, "ufo_bronze")


def load_ufo_source_data() -> None:
    """Extracts data from ufo source and loads data into delta table"""
    df = s.extract_html_table("test_source.html").rename(
        columns={"Date / Time": "DateTime"}
    )
    spark_df = s.create_spark_dataframe(df)
    s.load_to_table(spark_df, "./spark-warehouse/ufo/bronze")


if __name__ == "__main__":
    spark = s.spark_session("ufo", "local[*]")
    create_delta_tables(spark)
    load_ufo_source_data()
    df = spark.read.format("delta").load("./spark-warehouse/ufo/bronze")
    df.show()
