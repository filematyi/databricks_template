from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from databricks_boilerplate.tools.job_tools import jobcontroller
from databricks_boilerplate.domain.catalog_objects import raw_table, enriched_table, gold_table


@jobcontroller(
    inputs=[raw_table],
    outputs=[enriched_table]
)
def job_enriched_raw_table_with_date(spark: SparkSession, *tables: DataFrame) -> tuple[DataFrame, ...]:
    raw_table = tables[0]
    current_epoch_in_string = str(datetime.now())
    enriched_table = raw_table.withColumn("date", lit(current_epoch_in_string))
    return (enriched_table,)


@jobcontroller(
    inputs=[enriched_table],
    outputs=[gold_table]
)
def job_enriched_table_to_gold(spark: SparkSession, *tables: DataFrame) -> tuple[DataFrame, ...]:
    enriched_table = tables[0]
    gold_table = enriched_table.select("name")
    gold_table = gold_table.withColumn("gender", lit("male"))
    return (gold_table,)