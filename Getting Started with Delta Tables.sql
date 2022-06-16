-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px; padding-left:150px">
-- MAGIC   <img src="https://static1.squarespace.com/static/5bce4071ab1a620db382773e/t/5d266c78abb6d10001e4013e/1562799225083/appliedazuredatabricks3.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>
-- MAGIC 
-- MAGIC # Introducing <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">
-- MAGIC 
-- MAGIC #Setup
-- MAGIC The demos in this section all assume you have mounted a lake directory - in this case our mount is called "dblake". To follow the demos, please ensure you have an ADLS Gen 2 account mounted, and you are amending the lake path accordingly!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create an Empty Delta Table
-- MAGIC Here we are using SQL to show how easy it is to get started with Delta. We'll do a little cleanup to make sure we're starting fresh, then create an empty table structure

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ODSC2022

-- COMMAND ----------

DROP TABLE IF EXISTS ODSC2022.addresses

-- COMMAND ----------

CREATE TABLE ODSC2022.addresses (
  address string,
  current boolean,
  customerId int,
  effectiveDate string,
  endDate string
  )
USING DELTA
LOCATION '/tmp/DeltaLake/Address'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL Data Insertion
-- MAGIC Let's do some small insertions into the table and see what happens to our delta table

-- COMMAND ----------

insert into ODSC2022.addresses
select "A new customer address" as address, true as current, 11 customerID, "2020-08-03" as effectiveDate, null as endDate
union
select "A different address" as address, true as current, 41 customerID, "2020-08-03" as effectiveDate, null as endDate
union
select "Yet another address" as address, true as current, 58 customerID, "2020-08-03" as effectiveDate, null as endDate

-- COMMAND ----------

select * from ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We've got some data in our table, but let's go and have a look at the transaction log to see what's actually happened under the hood
-- MAGIC 
-- MAGIC <b>Go to the Lake:</b> Look at the delta folder - what's in the _delta_log? How many parquet files do we have?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data
-- MAGIC In the <i>beforetimes</i> we couldn't directly update data with sql, we would have to write convoluted scripts to read all the data, delete the original files then write the new version back down.
-- MAGIC 
-- MAGIC With delta, we have familiar SQL commands such as Delta to do this work for us!
-- MAGIC 
-- MAGIC Let's get some dummy data as a temporary view, then we can merge this into our original table - all still through SQL

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates
as
SELECT "An updated address" as address, true as current, 11 customerID, "2020-08-04" as effectiveDate, null as endDate

-- COMMAND ----------

MERGE INTO ODSC2022.addresses as original
USING updates
ON original.customerID = updates.customerID
WHEN MATCHED THEN
      UPDATE SET *
WHEN NOT MATCHED THEN
      INSERT *

-- COMMAND ----------

select * from ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once again, let's go and look at the lake to see how this has affected the transaction log
-- MAGIC 
-- MAGIC <b>Go to the Lake:</b> What change did the merge make to the _delta_log? How many parquet files do we now have?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Schema Evolution
-- MAGIC One of the primary drivers for people adopting lakes is the ability to have loose structures that evolve with the data, but having no schema enforcement often leads to corrupted data, poor data quality and other issues.
-- MAGIC 
-- MAGIC Delta allows for "managed schema evolution", where certain types of update can be automatically applied to the delta table, should we choose to allow it.
-- MAGIC 
-- MAGIC The following are considered "allowable schema drift":
-- MAGIC  - New columns
-- MAGIC  - Upscaled Data Types

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates
as
SELECT "Maybe another address" as address, true as current, 101 customerID, "2020-08-04" as effectiveDate, null as endDate, "United Kingdom" Country

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

MERGE INTO ODSC2022.addresses as original
USING updates
ON original.customerID = updates.customerID
WHEN MATCHED THEN
      UPDATE SET *
WHEN NOT MATCHED THEN
      INSERT *

-- COMMAND ----------

select * from ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying the Transaction Log
-- MAGIC So far we've been going to look at the actual json files to get familiar with the transaction log - if you want to quickly view the state of the table's history, this is accessible through a quick metadata query with the `describe history` SQL query

-- COMMAND ----------

describe detail ODSC2022.addresses

-- COMMAND ----------

describe history ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time Travel
-- MAGIC One of the major benefits of the way delta works, is the ability to view the state of the table at different states as it evolves. Old parquet files are not automatically deleted, thus we can inspect the table at any particular state.
-- MAGIC 
-- MAGIC This is achieved through two keywords in our query:
-- MAGIC  - `VERSION AS OF` returns a specific numbered version of the delta table
-- MAGIC  - `TIMESTAMP AS OF` returns the state of the delta table at a particular time

-- COMMAND ----------

select * from ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimizing Tables with <img width="200px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">
-- MAGIC 
-- MAGIC The optimize function will inspect our table to see if any of the underlying parquet files are too small to have efficient compression. If so, it will combine parquet files together into newer, better optimized parquet files. The data itself does not change - you can think of this as a maintenance task almost similar to defragmenting an index

-- COMMAND ----------

--First lets see how many underlying files in our table
describe detail ODSC2022.addresses

-- COMMAND ----------

-- Now let's run an optimize job on the table
optimize ODSC2022.addresses

-- COMMAND ----------

-- And we can look again to see the impact this has had
describe detail ODSC2022.addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleaning up History in <img width="200px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">
-- MAGIC 
-- MAGIC There is a LOT of redundant data writing behind how Delta works. Whilst this gives us a huge amount of power, flexibility and simplicity in our day-to-day operations, we don't want our storage costs to explode because of it.
-- MAGIC 
-- MAGIC We can run the `VACUUM` command on a delta table to delete any parquet files that were marked as 'removed' longer than a specified time period ago. For example, we might want to run a rolling 30 day vacuum, so whenever a file was made obsolete, we will remove it 30 days later
-- MAGIC 
-- MAGIC <b>NOTE:</b> Running the vacuum command means we can no longer use time travel beyond that point!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlContext.setConf("spark.databricks.delta.retentionDurationCheck.enabled","false")

-- COMMAND ----------

VACUUM ODSC2022.Addresses RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

VACUUM ODSC2022.Addresses RETAIN 0 HOURS

-- COMMAND ----------

describe detail ODSC2022.Addresses
