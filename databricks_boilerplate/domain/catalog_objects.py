from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from databricks_boilerplate.tools.objects import Catalog, Database, Format, StorageLocation, Table, Volume


catalog_location = StorageLocation(
    account_name="mfiledatabricksstorage2",
    container_name="unity-catalog-container",
    sub_path="",
)
raw_database_location = StorageLocation(
    account_name="mfiledatabricksstorage2",
    container_name="unity-catalog-container",
    sub_path="databases",
)
raw_table_location = StorageLocation(
    account_name="mfiledatabricksstorage2",
    container_name="external-data-container",
    sub_path="raw_tables",
)
enriched_table_location = StorageLocation(
    account_name="mfiledatabricksstorage2",
    container_name="external-data-container",
    sub_path="enriched_tables",
)
volume_location = StorageLocation(
    account_name="mfiledatabricksstorage2",
    container_name="databricks",
    sub_path="",
)

mfile_catalog = Catalog(
    name="mfile-catalog",
    storage_location=catalog_location,
)

raw_database = Database(
    name="raw_schema",
    catalog=mfile_catalog,
    storage_location=raw_database_location,
)
enriched_database = Database(
    name="enriched_schema",
    catalog=mfile_catalog,
    storage_location=raw_database_location,
)

raw_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="age", dataType=IntegerType(), nullable=False),
])
raw_table = Table(
    name="my_raw_table",
    table_schema=raw_schema,
    format=Format.DELTA,
    location=raw_table_location,
    catalog=mfile_catalog,
    database=raw_database
)

enriched_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="age", dataType=IntegerType(), nullable=False),
    StructField(name="date", dataType=StringType(), nullable=False),
])
enriched_table = Table(
    name="my_enriched_table",
    table_schema=enriched_schema,
    format=Format.DELTA,
    location=enriched_table_location,
    catalog=mfile_catalog,
    database=enriched_database
)


gold_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="gender", dataType=StringType(), nullable=False),
])
gold_table = Table(
    name="my_gold_table",
    table_schema=gold_schema,
    format=Format.DELTA,
    location=enriched_table_location,
    catalog=mfile_catalog,
    database=enriched_database
)

raw_volume = Volume(
    name="myvolume",
    location=volume_location,
    catalog=mfile_catalog,
    database=raw_database,
)