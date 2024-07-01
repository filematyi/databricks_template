from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from databricks_boilerplate.tools.entities import Catalog, Database, Format, StorageLocation, Table, Volume
from databricks_boilerplate.config import configuration

catalog_location = StorageLocation(
    account_name=configuration.unity_storage.account_name,
    container_name=configuration.unity_storage.container_name,
    sub_path="",
)
raw_database_location = StorageLocation(
    account_name=configuration.unity_storage.account_name,
    container_name=configuration.unity_storage.container_name,
    sub_path="",
)
raw_table_location = StorageLocation(
    account_name=configuration.external_storage.account_name,
    container_name=configuration.external_storage.container_name,
    sub_path="raw_tables",
)
enriched_table_location = StorageLocation(
    account_name=configuration.external_storage.account_name,
    container_name=configuration.external_storage.container_name,
    sub_path="enriched_tables",
)
volume_location = StorageLocation(
    account_name=configuration.ingress_storage.account_name,
    container_name=configuration.ingress_storage.container_name,
    sub_path="",
)

MFILE_CATALOG = Catalog(
    name="mfile-catalog",
    storage_location=catalog_location,
)

RAW_DATABASE = Database(
    name="raw_schema",
    catalog=MFILE_CATALOG,
    storage_location=raw_database_location,
)
ENRICHED_DATABASE = Database(
    name="enriched_schema",
    catalog=MFILE_CATALOG,
    storage_location=raw_database_location,
)

raw_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="age", dataType=IntegerType(), nullable=False),
])
RAW_TABLE = Table(
    name="my_raw_table",
    table_schema=raw_schema,
    format=Format.DELTA,
    location=raw_table_location,
    database=RAW_DATABASE,
)

enriched_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="age", dataType=IntegerType(), nullable=False),
    StructField(name="date", dataType=StringType(), nullable=False),
])
ENRICHED_TABLE = Table(
    name="my_enriched_table",
    table_schema=enriched_schema,
    format=Format.DELTA,
    location=enriched_table_location,
    database=ENRICHED_DATABASE,
)


gold_schema = StructType([
    StructField(name="name", dataType=StringType(), nullable=False),
    StructField(name="gender", dataType=StringType(), nullable=False),
])
GOLD_TABLE = Table(
    name="my_gold_table",
    table_schema=gold_schema,
    format=Format.DELTA,
    location=enriched_table_location,
    database=ENRICHED_DATABASE,
)

RAW_VOLUME = Volume(
    name="myvolume",
    location=volume_location,
    database=RAW_DATABASE,
)
