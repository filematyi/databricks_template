from copy import deepcopy
from pytest import fixture
from pyspark.sql import SparkSession
from databricks_boilerplate.tools.objects import Format, LocalLocation, Table


@fixture
def spark() -> SparkSession:
    return SparkSession\
        .builder\
        .master("local[*]")\
        .appName("localTests")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
        .config('spark.sql.sources.default', 'delta')\
        .config('spark.connect.extensions.relation.classes', 'io.delta.connect.DeltaRelationPlugin')\
        .config('spark.connect.extensions.command.classes', 'io.delta.connect.DeltaCommandPlugin')\
        .getOrCreate()\
        .getActiveSession()


def prepare_table_to_local_execution(table: Table, local_path: str) -> Table:
    copied_table = deepcopy(table)
    copied_table.location = LocalLocation(path=local_path)
    copied_table.format = Format.PARQUET

    return copied_table