# Databricks notebook source
# MAGIC %md
# MAGIC ## 0. Imports, Setup

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, explode
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC Loading the table schema 

# COMMAND ----------

# MAGIC %run "./pid_schema"

# COMMAND ----------

get_pid_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Read from the stream, create table**

# COMMAND ----------

JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'

df_buses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "buses") \
  .load()

schema_pid = get_pid_schema() 
base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*")

# COMMAND ----------

select_stream = base_buses.writeStream \
        .format("memory")\
        .queryName("mem_buses")\
        .outputMode("append")\
        .start()

# COMMAND ----------

JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'

df_regbuses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "regbuses") \
  .load()

schema_pid = get_pid_schema() 
base_regbuses = df_regbuses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*")

select_stream_reg = base_regbuses.writeStream \
        .format("memory")\
        .queryName("mem_regbuses")\
        .outputMode("append")\
        .start()

# COMMAND ----------

# MAGIC %md
# MAGIC How many rows are in the stream at the moment?

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from mem_buses limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from mem_regbuses

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table data_buses;
# MAGIC drop table data_regbuses

# COMMAND ----------

# MAGIC %md
# MAGIC Create from bus stream

# COMMAND ----------

# MAGIC %sql
# MAGIC create table data_buses select * from mem_buses;
# MAGIC create table data_regbuses select * from mem_regbuses;

# COMMAND ----------

# MAGIC %md
# MAGIC # Detection of the bus behind
# MAGIC ## 1. Creating table for queries
# MAGIC Firstly, let's make a new table with data needed for the detection:
# MAGIC - Arrival and departure time of buses
# MAGIC - delay of buses
# MAGIC - bus id and number (`short_route_name`) 
# MAGIC - type of vehicle (regional/regular bus)
# MAGIC - bus registration number
# MAGIC - id and name of the station 
# MAGIC 
# MAGIC We use data about the last stop, from topics: buses and regional buses.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table buses_city;
# MAGIC drop table buses_reg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table buses_city
# MAGIC   select properties.trip.gtfs.route_id as bus_id,
# MAGIC          properties.trip.gtfs.route_short_name as bus_number, 
# MAGIC          properties.trip.vehicle_registration_number as bus_registr_num,
# MAGIC          properties.trip.vehicle_type.description_en as bus,
# MAGIC          properties.last_position.last_stop.id as last_stop_id,
# MAGIC          properties.last_position.next_stop.id as next_stop_id,
# MAGIC          properties.trip.gtfs.trip_id as trip_id,
# MAGIC          properties.last_position.state_position as bus_state,
# MAGIC          properties.last_position.delay.actual as delay, 
# MAGIC          properties.last_position.origin_timestamp as current_time,
# MAGIC          properties.last_position.last_stop.arrival_time as schedule_last_stop_arrival, 
# MAGIC          properties.last_position.last_stop.departure_time as schedule_last_stop_departure,
# MAGIC          properties.last_position.next_stop.arrival_time as schedule_next_stop_arrival, 
# MAGIC          properties.last_position.next_stop.departure_time as schedule_next_stop_departure,
# MAGIC          geometry as bus_geo
# MAGIC   from data_buses;
# MAGIC   
# MAGIC create table buses_reg
# MAGIC   select properties.trip.gtfs.route_id as regbus_id,
# MAGIC          properties.trip.gtfs.route_short_name as regbus_number, 
# MAGIC          properties.trip.vehicle_registration_number as regbus_registr_num,
# MAGIC          properties.trip.vehicle_type.description_en as regbus,
# MAGIC          properties.last_position.last_stop.id as regbus_stop_id,
# MAGIC          properties.last_position.last_stop.arrival_time as regbus_arrival_time, 
# MAGIC          properties.last_position.last_stop.departure_time as regbus_departure_time,
# MAGIC          properties.last_position.delay.actual as regbus_delay, 
# MAGIC          geometry as regbus_geo
# MAGIC   from data_regbuses

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from buses_city limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from buses_reg limit 5;

# COMMAND ----------

# MAGIC %md 
# MAGIC Some data analysis:
# MAGIC - Does arrival_time and departure_time differ? -> YES, as expected

# COMMAND ----------

# MAGIC %sql
# MAGIC select regbus_number, regbus_stop_id, regbus_arrival_time, regbus_departure_time, regbus_delay
# MAGIC from buses_reg
# MAGIC where regbus_arrival_time <> regbus_departure_time

# COMMAND ----------

# MAGIC %md
# MAGIC Join the tables - using inner join since we want to detect station where collisions happen

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table buses;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table buses 
# MAGIC   select * 
# MAGIC   from buses_city 
# MAGIC   inner join buses_reg on buses_city.last_stop_id = buses_reg.regbus_stop_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from buses;

# COMMAND ----------

# MAGIC %md
# MAGIC **Looks like there are no common stations so we continue only with topic of buses.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detect buses, which may occur on one bus/tram/boat stop at the same time
# MAGIC 
# MAGIC * Collision happens when the first bus is on the bus stop and the following buses have time gap only 3 minutes or lower
# MAGIC * Collect places, where this happens, type of vehicle and short route name.
# MAGIC * Find top 10 collision places

# COMMAND ----------

# MAGIC %md
# MAGIC #### Detect event when any bus catches previous one at a specific stop:
# MAGIC ##### 1. Get buses which are currently at the bus stop
# MAGIC - `last_stop_id` = specific stop
# MAGIC - first bus is on the bus stop - `bus_state = "at bus"`
# MAGIC - get the time it is expected to depart from the station (with current delay): `current_time`

# COMMAND ----------

# MAGIC %sql
# MAGIC select bus_number, bus_registr_num, trip_id, last_stop_id, delay, current_time, bus_state
# MAGIC from buses_city
# MAGIC where bus_state = "at_stop" and last_stop_id = "U43Z2P"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Get following buses that have time gap 3 minutes and lowe
# MAGIC - `next_stop_id` = specific stop
# MAGIC - select arbirtrary `bus_state` (it can still be at the previous station or on the way to the current station)
# MAGIC - get the expected time to arrive to the next station as `schedule_next_stop_departure` + `delay` 

# COMMAND ----------

# MAGIC %sql
# MAGIC select bus_number, bus_registr_num, trip_id, next_stop_id, delay, current_time, schedule_next_stop_departure, bus_state
# MAGIC from buses_city
# MAGIC where next_stop_id = "U43Z2P"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Load stop names 
# MAGIC * from file stops.json obtained from pid website
# MAGIC * make a new table with the stop names

# COMMAND ----------

df_stops = spark.read\
        .option("multiline","true")\
        .option("inferSchema", "true")\
        .json("/FileStore/tables/stops_pid.json")

# Get rid of the nested arrays
stop_names = df_stops.select('stopGroups.fullName').collect()[0][0]
station_ids = df_stops.select('stopGroups.stops.gtfsIds').collect()[0][0]
station_ids = [[i[0] for i in lli] for lli in station_ids]
len(stop_names), len(station_ids)

# COMMAND ----------

datadf = [[name, id] for name, id in zip(stop_names, station_ids)]
columns = ["stop_name", "station_id"]
df_stops = spark.createDataFrame(data = datadf, schema = columns)
df_stops = df_stops.withColumn("station_id", explode("station_id"))
df_stops.show(5)

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table stops_names_table

# COMMAND ----------

df_stops.createOrReplaceTempView("stops_names_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,tram_arrival,tram_number,bus_arrival,bus_number from departures inner join stopID on departures.bus_id=stopID.stopid 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create dashboard
# MAGIC Create table for coordinates in order to visualize the dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table df_geo_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table df_geo_table
# MAGIC   SELECT 
# MAGIC     cast(data_buses.geometry.coordinates[0] as double) AS x,
# MAGIC     cast(data_buses.geometry.coordinates[1] as double) AS y
# MAGIC   FROM data_buses;

# COMMAND ----------

import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt

custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]' 
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter) 

# COMMAND ----------

df_geo_p.loc[:5] 

# COMMAND ----------

fig, ax = ox.plot_graph(G, show=False, close=False) 
df_geo = spark.sql("SELECT * FROM df_geo_table")
df_geo_p = df_geo.toPandas() 
x = df_geo_p.loc[1:300,'x'] 
y = df_geo_p.loc[1:300,'y'] 
ax.scatter(x, y, c='red') 
plt.show()

# COMMAND ----------


