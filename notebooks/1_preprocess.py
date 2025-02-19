# Databricks notebook source

from vg_sales.adapter.csv_io import CsvIO
from vg_sales.config.config_manager import ProjectConfig
from vg_sales.service.pipe.data_processor import data_processor_pipe
from vg_sales.service.transform.timestamp_col_spark import AddTimeStampCol
from vg_sales.service.util import create_session, enable_change_data_feed


def main():
    config = ProjectConfig.from_yaml(config_path="../project_config.yaml")
    output_schema_template = f"{config.catalog_name}.{config.schema_name}.{{0}}"
    output_schema = [output_schema_template.format(tbl_name) for tbl_name in ("train_set", "test_set")]
    spark = create_session()
    io_handler = CsvIO(spark=spark)

    try:
        pandas_data = io_handler.read(config.data_path, options={"header": True})
        preprocess_pipe = data_processor_pipe(
            cat_features=config.cat_features,
            num_features=config.num_features,
        )

        print("preprocessing")
        train_set, test_set = preprocess_pipe(pandas_data)

        train_set = AddTimeStampCol("update_timestamp_utc").apply(
            spark.createDataFrame(train_set),
        )
        test_set = AddTimeStampCol("update_timestamp_utc").apply(
            spark.createDataFrame(test_set),
        )

        io_handler.write(data=train_set, path=output_schema[0])
        io_handler.write(data=test_set, path=output_schema[1])

        print("success")
        (enable_change_data_feed(spark=spark, path=schema) for schema in output_schema)

    except Exception as e:
        print(e)
        exit(1)


if __name__ == "__main__":
    main()

# COMMAND ----------
