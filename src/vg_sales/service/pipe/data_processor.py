from vg_sales.service.base import Compose
from vg_sales.service.transform.convert_types import ConvertType
from vg_sales.service.transform.fill_na import FillNa
from vg_sales.service.transform.split_data import TrainTestSplit


def data_processor_pipe():
    return Compose(
        [
            ConvertType(
                [
                    "Platform",
                    "Genre",
                    "Publisher",
                ],
                "category",
            ),
            ConvertType(
                [
                    "NA_Sales",
                    "EU_Sales",
                    "JP_Sales",
                    "Other_Sales",
                    "Global_Sales",
                ],
                "double",
            ),
            FillNa(),
            TrainTestSplit(test_size=0.2),
        ]
    )
