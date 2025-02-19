import os

import pandas as pd

from vg_sales.config.config_manager import ProjectConfig
from vg_sales.service.pipe.data_processor import data_processor_pipe


def test_preprocessed_dtypes():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "../project_config.yaml")

    # Load config
    config = ProjectConfig.from_yaml(config_path)

    # Create sample data with expected columns
    sample_data = pd.DataFrame(
        {
            "Name": ["Game1", "Game2"],
            "Platform": ["PS4", "Xbox"],
            "Year_of_Release": [2020, 2021],
            "Genre": ["Action", "Sports"],
            "Publisher": ["EA", "Ubisoft"],
            "NA_Sales": [1.2, 2.3],
            "EU_Sales": [0.8, 1.5],
            "JP_Sales": [0.3, 0.1],
            "Other_Sales": [0.2, 0.4],
            "Global_Sales": [2.5, 4.3],
        }
    )

    # Apply preprocessing
    preprocess_pipe = data_processor_pipe(cat_features=config.cat_features, num_features=config.num_features)
    train_df, test_df = preprocess_pipe(sample_data)

    # Assert categorical features are of category dtype
    for cat_feature in config.cat_features:
        assert train_df[cat_feature].dtype == "category"
        assert test_df[cat_feature].dtype == "category"

    # Assert numerical features are of float dtype
    for num_feature in config.num_features:
        assert train_df[num_feature].dtype == "float64"
        assert test_df[num_feature].dtype == "float64"
