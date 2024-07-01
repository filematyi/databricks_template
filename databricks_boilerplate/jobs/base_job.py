from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from databricks_boilerplate.tools.job_tools import jobcontroller
from databricks_boilerplate.domain.catalog_objects import RAW_TABLE, ENRICHED_TABLE, GOLD_TABLE


@jobcontroller(
    inputs=[RAW_TABLE],
    outputs=[ENRICHED_TABLE],
)
def job_enriched_raw_table_with_date(spark: SparkSession, *tables: DataFrame) -> tuple[DataFrame, ...]:
    """Process raw data and save into enriched table."""
    raw_table = tables[0]
    current_epoch_in_string = str(datetime.now())
    enriched_table = raw_table.withColumn("date", lit(current_epoch_in_string))
    return (enriched_table,)


@jobcontroller(
    inputs=[ENRICHED_TABLE],
    outputs=[GOLD_TABLE],
)
def job_enriched_table_to_gold(spark: SparkSession, *tables: DataFrame) -> tuple[DataFrame, ...]:
    """Process enriched data and store in gold table."""
    enriched_table = tables[0]
    gold_table = enriched_table.select("name")
    gold_table = gold_table.withColumn("gender", lit("male"))
    return (gold_table,)
