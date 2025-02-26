import pandas as pd

from vg_sales.service.base import Transform


class FillNa(Transform):
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        imputation_dict = {
            "Platform": data["Platform"].mode()[0],
            "Genre": data["Genre"].mode()[0],
            "Publisher": data["Publisher"].mode()[0],
            "NA_Sales": data["NA_Sales"].mean(),
            "EU_Sales": data["EU_Sales"].mean(),
            "JP_Sales": data["JP_Sales"].mean(),
            "Other_Sales": data["Other_Sales"].mean(),
            "Global_Sales": data["Global_Sales"].mean(),
        }

        data = data.fillna(imputation_dict)
        return data
