-- Databricks notebook source
--1. All-Time Top Drivers by Wins
use catalog dltu11;
use schema gold_f1;

CREATE OR REPLACE MATERIALIZED VIEW gold_top_drivers_wins AS
SELECT
    d.DriverID,
    d.driversfullname,
    d.Nationality,
    COUNT(*) AS wins
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.drivers_silver d
    ON res.driver_id = d.DriverId
WHERE try_cast(res.final_position AS INT) = 1
GROUP BY d.DriverID, d.driversfullname,d.Nationality
having wins >= 10
ORDER BY wins DESC;


-- COMMAND ----------

--2. Fastest Average Lap Time by Driver in a Season

use catalog dltu11;
use schema gold_f1;

CREATE OR REPLACE MATERIALIZED VIEW gold_fastest_avg_lap_by_driver AS
SELECT
    r.Year,
    lt.Driver_ID,
    d.FirstName,
    d.LastName,
    AVG(lt.Milliseconds) AS avg_lap_time_ms
FROM dltu11.silver_f1.lap_times_silver lt
JOIN dltu11.silver_f1.races_silver r
    ON lt.Race_ID = r.Race_ID
JOIN dltu11.silver_f1.drivers_silver d
    ON lt.Driver_ID = d.DriverID
GROUP BY r.Year, lt.Driver_ID, d.FirstName, d.LastName
ORDER BY r.Year, avg_lap_time_ms ASC;


-- COMMAND ----------

use catalog dltu11;
use schema gold_f1;

--3. Best Constructors (Average Points Per Race)
CREATE OR REPLACE MATERIALIZED VIEW gold_best_constructors AS
SELECT
    c.Constructor_Id,
    c.Constructor_Name,
    AVG(res.points) AS avg_points_per_race
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.constructors_silver c    ON res.constructor_id = c.Constructor_Id

GROUP BY c.Constructor_Id, c.Constructor_Name
ORDER BY avg_points_per_race DESC;


-- COMMAND ----------

use catalog dltu11;
use schema gold_f1;

--4. Races with the Most Overtaking (Position Changes)
CREATE OR REPLACE MATERIALIZED VIEW gold_overtake_races AS
SELECT
    r.Race_ID,
    r.Race_Name,
    r.Year,
    SUM(ABS(res.final_position - res.grid_position)) AS total_overtakes
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.races_silver r
    ON res.race_id = r.Race_ID
GROUP BY r.Race_ID, r.Race_Name, r.Year
ORDER BY total_overtakes DESC;

-- COMMAND ----------

use catalog dltu11;
use schema gold_f1;

--5. Driver Qualifying vs Race Position Comparison
CREATE OR REPLACE MATERIALIZED VIEW  gold_qualifying_vs_race_performance AS
SELECT
    r.Year,
    d.DriverID,
    d.FirstName,
    d.LastName,
    q.qualifying_position,
    res.final_position,
    (q.qualifying_position - res.final_position) AS position_gain
FROM dltu11.silver_f1.qualifying_silver q
JOIN dltu11.silver_f1.results_silver res
    ON q.race_id = res.race_id AND q.driver_id = res.driver_id
JOIN dltu11.silver_f1.races_silver r
    ON q.race_id = r.Race_ID
JOIN dltu11.silver_f1.drivers_silver d
    ON q.driver_id = d.DriverID
ORDER BY r.Year, position_gain DESC;


-- COMMAND ----------

use catalog dltu11;
use schema gold_f1;

CREATE OR REFRESH MATERIALIZED VIEW driver_standings_gold
COMMENT 'Seasonal driver points, wins, and ranking for dashboards'
AS
SELECT
  r.Year AS Year,
  res.driver_id AS driver_id,
  d.FirstName,
  d.LastName,
  d.Nationality,
  SUM(res.points) AS total_points,
  SUM(CASE WHEN res.final_position = 1 THEN 1 ELSE 0 END) AS wins
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.races_silver r
  ON res.race_id = r.Race_ID
JOIN dltu11.silver_f1.drivers_silver d
  ON res.driver_id = d.DriverID
GROUP BY
  r.Year,
  res.driver_id,
  d.FirstName,
  d.LastName,
  d.Nationality
ORDER BY
  r.Year,
  total_points DESC;

-- COMMAND ----------

use catalog dltu11;
use schema gold_f1;
CREATE OR REFRESH MATERIALIZED VIEW driver_points_gold
SELECT
    r.Year AS Year,
    res.driver_id AS Driver_ID,
 d.driversfullname,
    d.Nationality,
    SUM(res.points) AS total_points
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.races_silver r
    ON res.race_id = r.Race_ID
JOIN dltu11.silver_f1.drivers_silver d
    ON res.driver_id = d.DriverID
GROUP BY
    r.Year,
    res.driver_id,
 d.driversfullname,
    d.Nationality
HAVING total_points > 0
ORDER BY

    total_points ASC;

-- COMMAND ----------



use catalog dltu11;
use schema gold_f1;
CREATE OR REFRESH MATERIALIZED VIEW driver_totalpoints_gold_ as
SELECT
    r.Year AS Year,
    res.driver_id AS Driver_ID,
 d.driversfullname,
    d.Nationality,
    SUM(res.points) AS total_points
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.races_silver r
    ON res.race_id = r.Race_ID
JOIN dltu11.silver_f1.drivers_silver d
    ON res.driver_id = d.DriverID
GROUP BY
    r.Year,
    res.driver_id,
 d.driversfullname,
    d.Nationality
HAVING total_points > 0
ORDER BY

    total_points ASC;

-- COMMAND ----------

CREATE MATERIALIZED VIEW dltu11.gold_f1_dashboard_mv AS
SELECT
    r.Race_ID,
    r.Year,
    r.Round,
    r.Race_Name,
    r.Date,
    r.Start_Time,
    c.Circuit_ID,
    c.Circuit_Name,
    c.Location,
    c.Country,
    c.Latitude,
    c.Longitude,
    c.Altitude,
    COUNT(p.Stop) AS total_pit_stops,
    AVG(p.Duration) AS avg_pit_stop_duration,
    MAX(p.Duration) AS max_pit_stop_duration,
    MIN(p.Duration) AS min_pit_stop_duration,
    COUNT(DISTINCT p.Driver_ID) AS drivers_with_pit_stops
FROM
    dltu11.silver_f1.races_silver r
JOIN
    dltu11.silver_f1.circuits_silver c
    ON r.Circuit_ID = c.Circuit_ID
LEFT JOIN
    dltu11.silver_f1.pit_stops_silver p
    ON r.Race_ID = p.Race_ID
GROUP BY
    r.Race_ID,
    r.Year,
    r.Round,
    r.Race_Name,
    r.Date,
    r.Start_Time,
    c.Circuit_ID,
    c.Circuit_Name,
    c.Location,
    c.Country,
    c.Latitude,
    c.Longitude,
    c.Altitude;
