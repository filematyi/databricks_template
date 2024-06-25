

from typing import Any
from databricks_boilerplate.domain.catalog_objects import raw_table, enriched_table
from databricks_boilerplate.jobs.base_job import job_enriched_raw_table_with_date, job_enriched_table_to_gold
from databricks_boilerplate.tools.actions import read_table_into_dataframe
from tests.conftest import prepare_table_to_local_execution


def test_job_enriched_raw_table_with_date(spark: Any) -> None:
    copied_raw_table = prepare_table_to_local_execution(
        table=raw_table,
        local_path="./tests/src/my_raw_table/my_raw_table",
    )
    raw_df = read_table_into_dataframe(spark=spark, table=copied_raw_table)
    original_job = job_enriched_raw_table_with_date._original
    results = original_job(spark, raw_df)

    assert len(results) == 1

    enriched_df = results[0]

    assert enriched_df.count() == 100


def test_job_enriched_table_to_gold(spark: Any) -> None:
    copied_enriched_table = prepare_table_to_local_execution(
        table=enriched_table,
        local_path="./tests/src/my_enriched_table/my_enriched_table",
    )
    enriched_df = read_table_into_dataframe(spark=spark, table=copied_enriched_table)
    original_job = job_enriched_table_to_gold._original
    results = original_job(spark, enriched_df)

    assert len(results) == 1

    enriched_df = results[0]

    assert enriched_df.count() == 100


def test_chain_job_steps(spark: Any) -> None:
    copied_raw_table = prepare_table_to_local_execution(
        table=raw_table,
        local_path="./tests/src/my_raw_table/my_raw_table",
    )
    raw_df = read_table_into_dataframe(spark=spark, table=copied_raw_table)
    enriched_df = job_enriched_raw_table_with_date._original(spark, raw_df)[0]

    golden_df = job_enriched_table_to_gold._original(spark, enriched_df)[0]

    assert golden_df.count() == 100