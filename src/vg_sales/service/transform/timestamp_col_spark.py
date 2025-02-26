from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, to_utc_timestamp

from vg_sales.service.base import Transform


class AddTimeStampCol(Transform):
    def __init__(self, col_name: str):
        self.col_name = col_name

    def apply(self, data: DataFrame) -> DataFrame:
        data = data.withColumn(
            self.col_name,
            to_utc_timestamp(current_timestamp(), "UTC"),
        )

        return data
