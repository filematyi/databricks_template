from databricks_boilerplate.tools.entities import Catalog, Database, LocalLocation, StorageLocation, Table, Volume, WriteMode
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.catalog import Catalog as SparkCatalog


def create_catalog(spark: SparkSession, catalog: Catalog) -> None:
    """Create new managed Catalog to dedicated location."""
    command = f"""
        CREATE CATALOG IF NOT EXISTS `{catalog.name}` MANAGED LOCATION '{catalog.storage_location.abfss_path()}';
    """
    spark.sql(command)


def create_database(spark: SparkSession, database: Database, managed_database: bool = True) -> None:
    """Create new managed database to dedicated location."""
    command = f"""
        CREATE DATABASE IF NOT EXISTS `{database.catalog.name}`.`{database.name}` {'MANAGED' if managed_database else ''} LOCATION '{database.storage_location.abfss_path()}';
    """
    spark.sql(command)


def create_table(spark: SparkSession, table: Table) -> None:
    """Create external table."""
    catalog = SparkCatalog(spark)
    if not catalog.tableExists(table.full_name):
        catalog.createTable(
            tableName=table.full_name,
            path=table.full_location,
            source=table.format.value,
            schema=table.table_schema,
        )


def create_volume(spark: SparkSession, volume: Volume) -> None:
    """Create new volume object."""
    command = f"""
        CREATE EXTERNAL VOLUME `{volume.catalog.name}`.`{volume.database.name}`.`{volume.name}` LOCATION '{volume.location.abfss_path()}'
    """
    spark.sql(command)


def insert_dataframe_to_table(dataframe: DataFrame, table: Table) -> None:
    """Insert dataframe's data to existing table.

    The method must differentiate between different data location and align the saving logic according to that.

    Allowed actions
    ---------------
    ADD COLUMN
        mergeSchema supports column addition

    Restricted actions
    ------------------
    DROP COLUMN
        table resinsert + data rewrite needed
    CHANGE DTPYE
        table reinsert needed

    """
    if isinstance(table.location, LocalLocation):
        dataframe.write.parquet(table.location.path)
    if isinstance(table.location, StorageLocation):
        dataframe.write.saveAsTable(
            name=table.full_name,
            format=table.format.value,
            mode=WriteMode.APPEND.value,
            partitionBy=table.partition_by,
            mergeSchema=True,
        )


def read_table_into_dataframe(spark: SparkSession, table: Table) -> DataFrame | None:
    """Read an existing table.

    The method must differentiate between different data location and align the reading logic accourding to that.
    """
    if isinstance(table.location, LocalLocation):
        return spark.read.format(table.format.value).load(table.location.path)
    if isinstance(table.location, StorageLocation):
        return spark.read.format(table.format.value).load(table.full_location)
    return None


def _schema_diff(spark: SparkSession, df_1: DataFrame, df_2: DataFrame) -> DataFrame:
    s1 = spark.createDataFrame(df_1.dtypes, ["d1_name", "d1_type"])
    s2 = spark.createDataFrame(df_2.dtypes, ["d2_name", "d2_type"])
    return (
        s1.join(s2, s1.d1_name == s2.d2_name, how="outer")
        .where((s1.d1_type.isNull()) | (s2.d2_type.isNull()) | (s1.d1_type != s2.d2_type))
        .select(s1.d1_name, s1.d1_type, s2.d2_name, s2.d2_type)
        .fillna("")
    )
