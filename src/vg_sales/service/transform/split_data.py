import pandas as pd
from numpy import double
from sklearn.model_selection import train_test_split

from vg_sales.service.base import Transform


class TrainTestSplit(Transform):
    def __init__(self, test_size: double, random_state: int = 42):
        self.test_size = test_size
        self.random_state = random_state

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        train_set, test_set = train_test_split(
            data,
            test_size=self.test_size,
            random_state=self.random_state,
        )
        return train_set, test_set
