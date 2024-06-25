from copy import deepcopy
from databricks_boilerplate.tools.actions import insert_dataframe_to_table
from databricks_boilerplate.tools.objects import LocalLocation
from pyspark.sql import Row, SparkSession
from databricks_boilerplate.domain.catalog_objects import enriched_schema, enriched_table

spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("localTests")\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
        .config('spark.sql.sources.default', 'delta')\
        .config('spark.connect.extensions.relation.classes', 'io.delta.connect.DeltaRelationPlugin')\
        .config('spark.connect.extensions.command.classes', 'io.delta.connect.DeltaCommandPlugin')\
        .getOrCreate()\
        .getActiveSession()

rows = [
    Row(name='jani', age=12, date='2024-12-12 15:16.777')
    for _ in range(100)
]
df = spark.createDataFrame(rows, enriched_schema)

copied_enriched_table = deepcopy(enriched_table)
copied_enriched_table.location = LocalLocation(path="./tests/src/my_enriched_table/my_enriched_table")
copied_enriched_table.format = 'parquet'

insert_dataframe_to_table(dataframe=df, table=copied_enriched_table)