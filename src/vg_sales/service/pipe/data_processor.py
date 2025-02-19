from vg_sales.service.base import Compose
from vg_sales.service.transform.convert_types import ConvertType
from vg_sales.service.transform.fill_na import FillNa
from vg_sales.service.transform.split_data import TrainTestSplit


def data_processor_pipe(cat_features: list[str], num_features: list[str]):
    return Compose(
        [
            ConvertType(
                cat_features,
                "category",
            ),
            ConvertType(
                num_features,
                "double",
            ),
            FillNa(),
            TrainTestSplit(test_size=0.2),
        ]
    )
