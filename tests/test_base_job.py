from typing import Any, Callable
from unittest.mock import patch
from databricks_boilerplate.domain.catalog_objects import RAW_TABLE, ENRICHED_TABLE
from databricks_boilerplate.jobs.base_job import job_enriched_raw_table_with_date, job_enriched_table_to_gold
from databricks_boilerplate.tools.actions import read_table_into_dataframe
from conftest import prepare_table_to_local_execution


N_ROW = 100

@patch("databricks_boilerplate.jobs.base_job.read_table_into_dataframe")
def test_job_enriched_raw_table_with_date(mocked_read_table_into_dataframe: Callable, spark: Any) -> None:

    copied_raw_table = prepare_table_to_local_execution(
        table=RAW_TABLE,
        local_path="./tests/src/my_raw_table/my_raw_table",
    )

    raw_df = read_table_into_dataframe(spark=spark, table=copied_raw_table)
    mocked_read_table_into_dataframe.return_value = raw_df
    results = job_enriched_raw_table_with_date(spark=spark, dry_run=True)

    assert len(results) == 1

    enriched_df = results[0]

    assert enriched_df.count() == N_ROW


@patch("databricks_boilerplate.jobs.base_job.read_table_into_dataframe")
def test_job_enriched_table_to_gold(mocked_read_table_into_dataframe: Callable, spark: Any) -> None:
    copied_enriched_table = prepare_table_to_local_execution(
        table=ENRICHED_TABLE,
        local_path="./tests/src/my_enriched_table/my_enriched_table",
    )
    enriched_df = read_table_into_dataframe(spark=spark, table=copied_enriched_table)
    mocked_read_table_into_dataframe.return_value = enriched_df
    results = job_enriched_table_to_gold(spark=spark, dry_run=True)

    assert len(results) == 1

    gold_df = results[0]

    assert gold_df.count() == N_ROW


@patch("databricks_boilerplate.jobs.base_job.read_table_into_dataframe")
def test_chain_job_steps(mocked_read_table_into_dataframe: Callable, spark: Any) -> None:
    copied_raw_table = prepare_table_to_local_execution(
        table=RAW_TABLE,
        local_path="./tests/src/my_raw_table/my_raw_table",
    )
    raw_df = read_table_into_dataframe(spark=spark, table=copied_raw_table)
    mocked_read_table_into_dataframe.return_value = raw_df
    
    enriched_df = job_enriched_raw_table_with_date(spark=spark, dry_run=True)[0]
    
    mocked_read_table_into_dataframe.return_value = enriched_df
    golden_df = job_enriched_table_to_gold(spark=spark, dry_run=True)[0]

    assert golden_df.count() == N_ROW
