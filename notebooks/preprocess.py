# Databricks notebook source

from vg_sales.adapter.csv_io import CsvIO
from vg_sales.service.pipe.data_processor import data_processor_pipe
from vg_sales.service.transform.timestamp_col_spark import AddTimeStampCol
from vg_sales.service.util import create_session


def main():
    # TODO get paths from config
    local_data_path = "dbfs:/Volumes/mlops_dev/sebastia/sebastian22/vgsales.csv"
    output_schema = "mlops_dev.sebastia.{}"
    spark = create_session()
    io_handler = CsvIO(spark=spark)

    try:
        pandas_data = io_handler.read(local_data_path, options={"header": True})
        preprocess_pipe = data_processor_pipe()

        print("preprocessing")
        train_set, test_set = preprocess_pipe(pandas_data)

        train_set = AddTimeStampCol("update_timestamp_utc").apply(
            spark.createDataFrame(train_set),
        )
        test_set = AddTimeStampCol("update_timestamp_utc").apply(
            spark.createDataFrame(test_set),
        )

        io_handler.write(data=train_set, path=output_schema.format("train_set"))
        io_handler.write(data=test_set, path=output_schema.format("test_set"))

        print("success")
        # enable_change_data_feed(spark=spark)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

# COMMAND ----------
