from typing import List

import pandas as pd

from vg_sales.service.base import Transform


class ConvertType(Transform):
    def __init__(self, columns: List[str], output_type: str):
        self.columns = columns
        self.output_type = output_type

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        for col in self.columns:
            data[col] = data[col].astype(self.output_type)

        return data
