# Databricks notebook source
spark.sql("Use catalog dltu11");
spark.sql("use  schema silver_f1");

# COMMAND ----------

import dlt
from pyspark.sql.functions import concat_ws

spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="lap_times_silver",
    comment="Transformed lap_times table for F1 analytics, with clear column names and audit columns included"
)
def lap_times_silver():
    df = spark.table("dltu11.bronze_f1.lap_times_bronze")
    df_transformed = (
        df.select(
            "raceId", "driverId", "lap", "position", "time", "milliseconds", 
            "ingestion_date", "source_file", "audit_source"
        ).toDF(
            "Race_ID", "Driver_ID", "Lap", "Position", "Time", "Milliseconds", 
            "Ingestion_Date", "Source_File", "Audit_Source"
        )
    )

    return df_transformed

# COMMAND ----------

import dlt

# Set your target catalog and schema
spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="constructors_silver",
    comment="Transformed lap_times table for F1 analytics, with clear column names and audit columns included"
)
def lap_times_silver():
    df = spark.table("dltu11.bronze_f1.constructors_bronze")
    
    # Rename columns for clarity and consistency
    df_transformed = (
        df.select(
            "constructorId", "constructorRef", "name", "nationality"
        ).toDF(
             "Constructor_Id", "Constructor_Ref", "Constructor_Name", "Constructor_Nationality"
        )
    )
    return df_transformed

# COMMAND ----------

import dlt
from pyspark.sql.functions import concat_ws

# Set your target catalog and schema
spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="drivers_silver",
    comment="Transformed drivers table for F1 analytics, with clear column names and audit columns included"
)
def drivers_silver():
    df = spark.table("dltu11.bronze_f1.drivers_bronze")
    
    # Rename columns for clarity and consistency
    df_transformed = (
        df.select(
            "driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality"
        ).toDF(
            "DriverId", "DriverRef", "DriverNumber", "DriverCode", "FirstName", "LastName", "DOB", "Nationality"
        )
    )
    # Create DriversFullName by concatenating FirstName and LastName
    df_transformed = df_transformed.withColumn(
        "DriversFullName",
        concat_ws(" ", df_transformed.FirstName, df_transformed.LastName)
    )
    return df_transformed

# COMMAND ----------

import dlt

# Set your target catalog and schema
spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="circuits_silver",
    comment="Transformed circuits table for F1 analytics, with clear column names and audit columns included"
)
def circuits_silver():
    df = spark.table("dltu11.bronze_f1.circuits_bronze")
    
    # Rename columns to clear analytics-focused names; keep audit columns as is
    df_transformed = (
        df.select(
            "circuitId", "circuitRef", "name", "location", "country", 
            "lat", "lng", "alt", "url",
            "ingestion_date", "source_file", "audit_source"
        ).toDF(
            "Circuit_ID", "Circuit_Ref", "Circuit_Name", "Location", "Country", 
            "Latitude", "Longitude", "Altitude", "URL",
            "Ingestion_Date", "Source_File", "Audit_Source"
        )
    )
    return df_transformed


# COMMAND ----------

import dlt

# Set your target catalog and schema
spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="pit_stops_silver",
    comment="Transformed pit_stops table for F1 analytics, with analytics-friendly column names and audit columns included"
)
def pit_stops_silver():
    df = spark.table("dltu11.bronze_f1.pit_stops_bronze")
    
    # Rename columns for clarity and include audit columns
    df_transformed = (
        df.select(
            "raceId", "driverId", "stop", "lap", "time", "duration", "milliseconds",
            "ingestion_date", "source_file", "audit_source"
        ).toDF(
            "Race_ID", "Driver_ID", "Stop", "Lap", "Time", "Duration", "Milliseconds",
            "Ingestion_Date", "Source_File", "Audit_Source"
        )
    )
    return df_transformed


# COMMAND ----------

import dlt

# Set your target catalog and schema
spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="races_silver",
    comment="Transformed races table for F1 analytics, with clear column names and audit fields"
)
def races_silver():
    df = spark.table("dltu11.bronze_f1.races_bronze")
    
    # Rename columns for clarity and include audit columns
    df_transformed = (
        df.select(
            "raceId", "year", "round", "circuitId", "name", "date", "time", "url", 
            "fp1_date", "fp1_time", "fp2_date", "fp2_time", 
            "fp3_date", "fp3_time", "quali_date", "quali_time",
            "sprint_date", "sprint_time",
            "ingestion_date", "source_file", "audit_source"
        ).toDF(
            "Race_ID", "Year", "Round", "Circuit_ID", "Race_Name", "Date", "Start_Time", "URL",
            "FP1_Date", "FP1_Time", "FP2_Date", "FP2_Time",
            "FP3_Date", "FP3_Time", "Quali_Date", "Quali_Time",
            "Sprint_Date", "Sprint_Time",
            "Ingestion_Date", "Source_File", "Audit_Source"
        )
    )
    return df_transformed


# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="results_silver",
    comment="Cleaned results data for F1 analytics, with analytics-friendly column names and audit columns"
)
def results_silver():
    df = spark.table("dltu11.bronze_f1.results_bronze")
    df_cleaned = (
        df.select(
            col("resultId").alias("result_id"),
            col("raceId").alias("race_id"),
            col("driverId").alias("driver_id"),
            col("constructorId").alias("constructor_id"),
            col("number").alias("driver_number"),
            col("grid").alias("grid_position"),
            col("position").alias("final_position"),
            col("positionText").alias("position_text"),
            col("positionOrder").alias("position_order"),
            col("points").alias("points"),
            col("laps").alias("laps"),
            col("time").alias("race_time"),
            col("milliseconds").alias("milliseconds"),
            col("fastestLap").alias("fastest_lap"),
            col("rank").alias("lap_rank"),
            col("fastestLapTime").alias("fastest_lap_time"),
            col("fastestLapSpeed").alias("fastest_lap_speed"),
            col("statusId").alias("status_id"),
            col("ingestion_date"),
            col("source_file"),
            col("audit_source")
        )
    )
    return df_cleaned


# COMMAND ----------

import dlt
from pyspark.sql.functions import col

spark.sql("USE CATALOG dltu11")
spark.sql("USE SCHEMA silver_f1")

@dlt.table(
    name="qualifying_silver",
    comment="Cleaned qualifying results for F1 analytics, with clear column names and audit columns"
)
def qualifying_silver():
    df = spark.table("dltu11.bronze_f1.status_bronze")
    df_cleaned = (
        df.select(
            col("qualifyId").alias("qualify_id"),
            col("raceId").alias("race_id"),
            col("driverId").alias("driver_id"),
            col("constructorId").alias("constructor_id"),
            col("number").alias("car_number"),
            col("position").alias("qualifying_position"),
            col("q1").alias("q1_time"),
            col("q2").alias("q2_time"),
            col("q3").alias("q3_time"),
            col("ingestion_date"),
            col("source_file"),
            col("audit_source")
        )
    )
    return df_cleaned