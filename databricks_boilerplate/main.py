import argparse
from pydantic import BaseModel
from pyspark.sql import SparkSession

from databricks_boilerplate.domain.catalog_objects import mfile_catalog, raw_database, enriched_database, raw_table, enriched_table, raw_volume
from databricks_boilerplate.jobs.base_job import job_enriched_raw_table_with_date, job_enriched_table_to_gold
from databricks_boilerplate.tools.actions import create_catalog, create_database, create_table, create_volume


class JobParameters(BaseModel):
    """Wrapper object to store job parameters."""

    job_name: str
    dry_run: bool

    @staticmethod
    def parse_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument("-n", "--job_name", type=str)
        parser.add_argument("-d", "--dry_run", action="store_true")

        args = parser.parse_args()
        print(args)

        return JobParameters(
            job_name=args.job_name,
            dry_run=args.dry_run
        )


def _create_objects_if_not_exists(spark: SparkSession):
    create_catalog(spark, mfile_catalog)
    create_database(spark, raw_database)
    create_database(spark, enriched_database)

    create_table(spark, raw_table)
    create_table(spark, enriched_table)

    create_volume(spark, raw_volume)


def entry_point() -> None:
    job_parameters = JobParameters.parse_arguments()

    spark = SparkSession\
        .builder\
        .master("local")\
        .appName("localTests")\
        .getOrCreate()\
        .getActiveSession()

    if job_parameters.job_name == "make_gold_job":
        job_enriched_table_to_gold(spark=spark)
    if job_parameters.job_name == "enrich_raw_job":
        job_enriched_raw_table_with_date(spark=spark)
    if job_parameters.job_name == "catalog_initialization":
        _create_objects_if_not_exists(spark=spark)
