from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from vg_sales.port.dataframe_io import DataFrameIO


class CsvIO(DataFrameIO):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str, options: Dict[str, str]) -> pd.DataFrame:
        df = self.spark.read.options(**options).format("csv").load(path)
        return df.toPandas()

    def write(self, data: DataFrame, path: str, mode: str = "append") -> None:
        data.write.mode(mode).saveAsTable(path)
