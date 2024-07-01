from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from databricks_boilerplate.domain.catalog_objects import RAW_TABLE, ENRICHED_TABLE, GOLD_TABLE
from databricks_boilerplate.tools.actions import read_table_into_dataframe, insert_dataframe_to_table


def job_enriched_raw_table_with_date(spark: SparkSession, dry_run: bool) -> tuple[DataFrame, ...]:
    """Process raw data and save into enriched table."""
    raw_table = read_table_into_dataframe(spark=spark, table=RAW_TABLE)

    current_epoch_in_string = str(datetime.now())
    enriched_table = raw_table.withColumn("date", lit(current_epoch_in_string))

    if not dry_run:
        insert_dataframe_to_table(enriched_table, ENRICHED_TABLE)
    return (enriched_table,)


def job_enriched_table_to_gold(spark: SparkSession, dry_run: bool) -> tuple[DataFrame, ...]:
    """Process enriched data and store in gold table."""
    enriched_table = read_table_into_dataframe(spark=spark, table=ENRICHED_TABLE)

    gold_table = enriched_table.select("name")
    gold_table = gold_table.withColumn("gender", lit("male"))

    if not dry_run:
        insert_dataframe_to_table(gold_table, GOLD_TABLE)
    return (gold_table,)
