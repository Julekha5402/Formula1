# Databricks notebook source
spark.sql("Use catalog dltu11");
spark.sql("use schema bronze_f1");

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit, col


# DRIVERS
@dlt.table(
    name="drivers_bronze",
    comment="Raw drivers data for Formula 1",
    table_properties={"quality": "f1 bronze"}
)
def drivers_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/Drivers")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )

# CONSTRUCTORS
@dlt.table(
    name="constructors_bronze",
    comment="Raw constructors data for Formula 1",
    table_properties={"quality": "bronze"}
)
def constructors_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/Constructors")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )

# COMMAND ----------

# CIRCUITS
@dlt.table(
  name="circuits_bronze",
  comment="Raw circuits data for Formula 1",
  table_properties={"quality": "bronze"}
)
def circuits_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/Circuits")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )
 
# RACES
@dlt.table(
  name="races_bronze",
  comment="Raw races data for Formula 1",
  table_properties={"quality": "bronze"}
)
def races_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/races")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )
 
# LAP TIMES
@dlt.table(
  name="lap_times_bronze",
  comment="Raw lap times data for Formula 1",
  table_properties={"quality": "bronze"}
)
def lap_times_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/lap_times")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )
 

# COMMAND ----------

# PIT STOPS

@dlt.table(
    name="pit_stops_bronze",
    comment="Raw pit stops data for Formula 1",
    table_properties={"quality": "bronze"}
)
def pit_stops_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/pit_stops")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )

# RESULTS

@dlt.table(
    name="results_bronze",
    comment="Raw results data for Formula 1",
    table_properties={"quality": "bronze"}
)
def results_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/results")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )

# QUALIFYING

@dlt.table(
    name="status_bronze",
    comment="Raw status data for Formula 1",
    table_properties={"quality": "bronze"}
)
def status_bronze():
    return (
        spark.read.option("header", True).parquet("/Volumes/dltu11/bronze/circuitv/Ops/F1datasets/qualifying")
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("audit_source", lit("batch_load"))
    )