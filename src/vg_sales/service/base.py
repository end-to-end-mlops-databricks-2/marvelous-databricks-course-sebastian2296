from functools import reduce

import pandas as pd
from pandas import DataFrame


class Transform:
    @classmethod
    def apply(cls, data: DataFrame) -> DataFrame:
        pass

    def __call__(self, data: DataFrame):
        return self.apply(data)


class Compose(Transform):
    def __init__(self, transforms: Transform):
        self.transforms = transforms

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        return reduce(lambda acc, t: t.apply(acc), self.transforms, data)
