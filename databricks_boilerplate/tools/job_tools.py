from typing import Callable

from databricks_boilerplate.tools.actions import insert_dataframe_to_table, read_table_into_dataframe


def jobcontroller(inputs: list, outputs: list):
    def decorator(func: Callable):
        def wrapper(spark, *args, **kwargs):
            dataframes = [read_table_into_dataframe(spark=spark, table=table) for table in inputs]
            result_dataframes = func(spark, *dataframes)
            for table, df in zip(outputs, result_dataframes):
                insert_dataframe_to_table(dataframe=df, table=table)
            return result_dataframes
        wrapper._original = func
        return wrapper
    return decorator