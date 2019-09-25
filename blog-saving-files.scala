// Databricks notebook source
// MAGIC %md
// MAGIC # Introduction
// MAGIC 
// MAGIC This notebook contains code to accompany my blog post titled "How You Should Save The Output Of Your Spark ETL Jobs".

// COMMAND ----------

// MAGIC %md
// MAGIC # Libraries

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %sh
// MAGIC apt-get install tree

// COMMAND ----------

// MAGIC %md
// MAGIC # Data

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We will use the `asa/airlines` dataset because it is huge. 
// MAGIC 
// MAGIC The schema is defined [here](http://stat-computing.org/dataexpo/2009/the-data.html).

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /databricks-datasets/asa/airlines

// COMMAND ----------

// MAGIC %md
// MAGIC Read all the files:

// COMMAND ----------

val filePath = "dbfs:/databricks-datasets/asa/airlines/*.csv"
val df = spark.read
  .option("header", true)
  .csv(filePath)
  .filter("Year >= 2006")
  .withColumn("Year", expr("cast(year as integer)"))
  .withColumn("Month", expr("cast(Month as integer)"))
  .withColumn("DayOfMonth", expr("cast(DayOfMonth as integer)"))
  .cache

df.printSchema

// COMMAND ----------

// check distribution of daily data
display(df.groupBy("Year", "Month", "DayOfMonth").count.orderBy("Year", "Month", "DayOfMonth"))

// COMMAND ----------

// MAGIC %md
// MAGIC Create directories to store `df` by folders and partitions:

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /tmp/asa/folders

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /tmp/asa/partitions

// COMMAND ----------

// MAGIC %md
// MAGIC Save `df` in year, month, day folders:

// COMMAND ----------

val folders = df
  .select("Year", "Month", "DayOfMonth")
  .distinct
  .as[(Integer, Integer, Integer)]
  .collect

// COMMAND ----------

folders.par.foreach {
  case (year, month, day) => df.filter($"Year" === year)
                               .filter($"Month" === month)
                               .filter($"DayOfMonth" === day)
                               .write
                               .mode("overwrite")
                               .option("header", true)
                               .csv(s"/tmp/asa/folders/$year/$month/$day/data.csv")
}

// COMMAND ----------

// MAGIC %sh
// MAGIC tree -v -P *csv /dbfs/tmp/asa/folders/2006

// COMMAND ----------

// MAGIC %md
// MAGIC Save `df` in year, month, day partitions:

// COMMAND ----------

df
  .write
  .mode("overwrite")
  .option("header", true)
  .partitionBy("Year", "Month", "DayOfMonth")
  .csv("/tmp/asa/partitions/data.csv")

// COMMAND ----------

// MAGIC %sh
// MAGIC tree -v -P *csv /dbfs/tmp/asa/partitions/data.csv

// COMMAND ----------

// MAGIC %md
// MAGIC # Adding New Data

// COMMAND ----------

// MAGIC %md
// MAGIC Add data for 2005 using partitions method:

// COMMAND ----------

val filePath = "dbfs:/databricks-datasets/asa/airlines/2005.csv"
val df2005 = spark.read
  .option("header", true)
  .csv(filePath)
  .withColumn("Year", expr("cast(year as integer)"))
  .withColumn("Month", expr("cast(Month as integer)"))
  .withColumn("DayOfMonth", expr("cast(DayOfMonth as integer)"))
  .write
  .mode("overwrite")
  .option("header", true)
  .partitionBy("Year", "Month", "DayOfMonth")
  .csv("/tmp/asa/partitions/data2005.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Move from `data2005.csv` to `data.csv`:

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /tmp/asa/partitions/data2005.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp -r /tmp/asa/partitions/data2005.csv/Year=2005 /tmp/asa/partitions/data.csv/Year=2005

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /tmp/asa/partitions/data.csv

// COMMAND ----------

// MAGIC %md
// MAGIC # Query data

// COMMAND ----------

// MAGIC %md
// MAGIC ## Partitions method

// COMMAND ----------

val Jan2008 = $"Year" === 2008 and $"Month" === 1

val df = spark.read.option("header", true)
  .csv("/tmp/asa/partitions/data.csv")
  .filter(Jan2008)

df.count

// COMMAND ----------

val firstHalfOf2008 = $"Year" === 2008 and $"Month".between(1, 6)

val df = spark.read.option("header", true)
  .csv("/tmp/asa/partitions/data.csv")
  .filter(firstHalfOf2008)

df.count

// COMMAND ----------

val yearEnd = $"Year".between(2007, 2008) and $"Month" === 12 and $"DayOfMonth".between(25, 31)

val df = spark.read.option("header", true)
  .csv("/tmp/asa/partitions/data.csv")
  .filter(yearEnd)

df.count

// COMMAND ----------

// MAGIC %md
// MAGIC ## Folders method

// COMMAND ----------

val df = spark.read.option("header", true)
  .csv("/tmp/asa/folders/2008/1/*/*")

df.count

// COMMAND ----------

val files = (1 to 6).map(m => s"/tmp/asa/folders/2008/$m/*/*")

val df = spark.read.option("header", true)
  .csv(files:_*)

df.count

// COMMAND ----------

// MAGIC %md
// MAGIC If there a difference between reading all 12 months of 2008 and then filter for 6 months or just read the 6 months?

// COMMAND ----------

// read all then filter
spark.read.option("header", true)
  .csv("/tmp/asa/folders/2008/*/*/*")
  .filter($"Month".between(1, 6))
  .count

// COMMAND ----------

spark.read.option("header", true)
  .csv("/tmp/asa/folders/*/12/*/*/*")
  .filter($"DayOfMonth".between(25, 31))
  .filter($"Year".between(2007, 2008))
  .count

// COMMAND ----------

val years = 2007 to 2008
val days = 25 to 31

val yearAndDays = years.flatMap(y => days.map(d => (y, d)))

val files = yearAndDays.map(x => s"/tmp/asa/folders/${x._1}/12/${x._2}/*")

spark.read.option("header", true)
  .csv(files:_*)
  .count

// COMMAND ----------

// what happens if the date does not exist?
val years = 1990 to 2008
val days = 25 to 31

val yearAndDays = years.flatMap(y => days.map(d => (y, d)))

val files = yearAndDays.map(x => s"/tmp/asa/folders/${x._1}/12/${x._2}/*")

spark.read.option("header", true)
  .csv(files:_*)
  .count

// COMMAND ----------

// MAGIC %md
// MAGIC # Clean Up

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /tmp

// COMMAND ----------

// MAGIC %fs
// MAGIC rm -r /tmp/asa
