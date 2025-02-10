from databricks.connect import DatabricksSession


def create_session():
    spark = DatabricksSession.builder.clusterId("0201-223057-c9uew8cc").getOrCreate()
    return spark


def enable_change_data_feed(spark: DatabricksSession, path: str) -> None:
    spark.sql(
        f"ALTER TABLE {path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);",
    )
