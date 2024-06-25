from databricks_boilerplate.tools.objects import Catalog, Database, LocalLocation, Table, Volume, WriteMode
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Catalog as SparkCatalog


def create_catalog(spark: SparkSession, catalog: Catalog):
    command = f"""
        CREATE CATALOG IF NOT EXISTS `{catalog.name}` MANAGED LOCATION '{catalog.storage_location.abfss_path()}';
    """
    spark.sql(command)


def create_database(spark: SparkSession, database: Database, managed_database: bool = True):
    command = f"""
        CREATE DATABASE IF NOT EXISTS `{database.catalog.name}`.`{database.name}` {'MANAGED' if managed_database else ''} LOCATION '{database.storage_location.abfss_path()}';
    """
    spark.sql(command)


def create_table(spark: SparkSession, table: Table):
    catalog = SparkCatalog(spark)
    if not catalog.tableExists(table.full_name):
        catalog.createTable(
            tableName=table.full_name,
            path=table.full_location,
            source=table.format.value,
            schema=table.table_schema
        )

def create_volume(spark: SparkSession, volume: Volume) -> None:
    command = f"""
        CREATE EXTERNAL VOLUME `{volume.catalog.name}`.`{volume.database.name}`.`{volume.name}` LOCATION '{volume.location.abfss_path()}'
    """
    spark.sql(command)


def insert_dataframe_to_table(dataframe: DataFrame, table: Table) -> None:
    if isinstance(table.location, LocalLocation):
        dataframe.write.parquet(table.location.path)
    else:
        dataframe.write.saveAsTable(
            name=table.full_name,
            format=table.format.value,
            mode=WriteMode.APPEND.value,
            partitionBy=table.partition_by,
        )


def read_table_into_dataframe(spark: SparkSession, table: Table) -> DataFrame:
    if isinstance(table.location, LocalLocation):
        return spark.read.format(table.format.value).load(table.location.path)

    return spark.read.format(table.format.value).load(table.full_location)
