from delta import DeltaTable
from app_utils import LOG_FILE_NAME, setup_logger
from pyspark.sql import DataFrame

logger = setup_logger("table_manager", LOG_FILE_NAME)


class TableManager:
    def __init__(self, spark):
        self.spark = spark

    def optimize_table(self, table_path: str) -> DeltaTable:
        """
        Coalesces small files into larger ones.

        Args:
            table_path (str): The path to the Delta table.

        Returns:
            DeltaTable: The optimized DeltaTable object.
        """
        try:
            deltatable = (
                DeltaTable.forPath(self.spark, table_path)
                .optimize()
                .executeCompaction()
            )
            logger.info(f"Coalescing files @ {table_path}")
            return deltatable
        except Exception as e:
            logger.info(f"{e}")

    def delta_vacuum(self, table_path: str, retention: int) -> DeltaTable:
        """Removes old files based on retention period.

        Args:
            table_path (str): The path to the Delta table.
            retention (int): The retention period in hours.

        Returns:
            DeltaTable: The vacuumed DeltaTable object.
        """
        try:
            deltaTable = DeltaTable.forPath(self.spark, table_path)
            logger.info(f"Vacuuming data after optimization @ {table_path}")
            return deltaTable.vacuum(retention)
        except Exception as e:
            logger.error(f"{e}")

    def load_data(self, df: DataFrame, table_path: str) -> DeltaTable:
        """Loads source data into Delta table.

        Args:
            df (DataFrame): The DataFrame containing the data to be loaded.
            table_path (str): The path to the Delta table.

        Returns:
            DeltaTable: The DeltaTable object after loading the data.
        """
        try:
            df.write.format("delta").mode("overwrite").save(table_path)
            logger.info(f"Data successfully loaded into delta lake @ {table_path}")
            self.optimize_table(table_path)
            return self.delta_vacuum(table_path, 0)
        except Exception as e:
            logger.error(f"{e}")

    def create_table(
        self, table_name: str, columns: list, table_path: str
    ) -> DeltaTable:
        """
        Creates a Delta table with the specified table name, columns, and path.

        Args:
            table_name (str): The name of the Delta table.
            columns (list): A list of tuples representing the columns. Each tuple should contain
                            the column name as the first element and the column data type as the second element.
            table_path (str): The path where the Delta table will be created.

        Returns:
            DeltaTable: The created DeltaTable object.
        """
        try:
            if DeltaTable.isDeltaTable(self.spark, f"{table_path}"):
                logger.info(
                    f"Delta table: '{table_name}' already exists @ '{table_path}'"
                )
            else:
                table_builder = (
                    DeltaTable.createIfNotExists(self.spark)
                    .tableName(table_name)
                    .location(table_path)
                )
                for column in columns:
                    table_builder = table_builder.addColumn(column[0], column[1])
                table = table_builder.execute()
                logger.info(f"Delta table: {table_name} created  @ '{table_path}'")
                return table
        except Exception as e:
            print(f"{e}")
