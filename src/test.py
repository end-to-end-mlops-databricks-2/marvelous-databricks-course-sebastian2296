# Databricks notebook source
"databricks remote execution test"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

spark.read.option("header", True).csv(
 "dbfs:/Volumes/mlops_dev/sebastia/sebastian22/vgsales.csv"
).show()

# COMMAND ----------
