from delta import DeltaTable
from app_utils import LOG_FILE_NAME, setup_logger
from pyspark.sql import SparkSession


logger = setup_logger("tables", LOG_FILE_NAME)


# TODO: Create test
def create_ufo_bronze_table(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates a bronze ufo table"""
    try:
        path = "./lakehouse/ufo/bronze"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .tableName(table_name)
                .addColumn("DateTime", "STRING")
                .addColumn("City", "STRING")
                .addColumn("State", "STRING")
                .addColumn("Country", "STRING")
                .addColumn("Shape", "STRING")
                .addColumn("Duration", "STRING")
                .addColumn("Summary", "STRING")
                .addColumn("Posted", "STRING")
                .addColumn("Images", "STRING")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")


def create_ufo_silver_table(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates a silver ufo table"""
    try:
        path = "./lakehouse/ufo/silver"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .tableName(table_name)
                .addColumn("id", "LONG")
                .addColumn("id_location", "LONG")
                .addColumn("city", "STRING")
                .addColumn("state", "STRING")
                .addColumn("country", "STRING")
                .addColumn("id_description", "LONG")
                .addColumn("shape", "STRING")
                .addColumn("duration", "STRING")
                .addColumn("summary", "STRING")
                .addColumn("images", "STRING")
                .addColumn("id_date", "LONG")
                .addColumn("date", "DATE")
                .addColumn("year", "INT")
                .addColumn("month", "INT")
                .addColumn("dayofweek", "STRING")
                .addColumn("week", "INT")
                .addColumn("hour", "INT")
                .addColumn("id_astro", "LONG")
                .addColumn("moonPhaseAngle", "DOUBLE")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")

def create_ufo_gold_dim_location(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates dimension location"""
    try:
        path = "./lakehouse/ufo/gold/dim_location"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .tableName(table_name)
                .addColumn("id_location", "LONG")
                .addColumn("city", "STRING")
                .addColumn("state", "STRING")
                .addColumn("country", "STRING")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")

def create_ufo_gold_dim_description(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates dimension description"""
    try:
        path = "./lakehouse/ufo/gold/dim_description"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .addColumn("id_description", "LONG")
                .addColumn("shape", "STRING")
                .addColumn("duration", "STRING")
                .addColumn("summary", "STRING")
                .addColumn("images", "STRING")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")

def create_ufo_gold_dim_date(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates dimension date"""
    try:
        path = "./lakehouse/ufo/gold/dim_date"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .addColumn("id_date", "LONG")
                .addColumn("date", "DATE")
                .addColumn("year", "INT")
                .addColumn("month", "INT")
                .addColumn("dayofweek", "STRING")
                .addColumn("week", "INT")
                .addColumn("hour", "INT")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")

def create_ufo_gold_dim_astro(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates dimension astro"""
    try:
        path = "./lakehouse/ufo/gold/dim_astro"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .addColumn("id_astro", "LONG")
                .addColumn("moonPhaseAngle", "DOUBLE")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")

def create_ufo_gold_fact(spark: SparkSession, table_name: str) -> DeltaTable:
    """Creates dimension fact"""
    try:
        path = "./lakehouse/ufo/gold/fact"

        if DeltaTable.isDeltaTable(spark, f"{path}"):
            logger.info(f"Delta table: '{table_name}' already exists @ '{path}'")
        else:
            table = (
                DeltaTable.createIfNotExists(spark)
                .addColumn("id_location", "LONG")
                .addColumn("id_description", "LONG")
                .addColumn("id_date", "LONG")
                .addColumn("id_astro", "LONG")
                .location(f".{path}")
                .execute()
            )

            logger.info(f"Created delta table: {table_name} located @ '{path}'")

            return table
    except Exception as e:
        logger.error(f"{e}")