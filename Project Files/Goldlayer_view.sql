-- Databricks notebook source
USE CATALOG dltu11;
use schema gold_f1;

CREATE OR REPLACE MATERIALIZED VIEW dashboard_f1_summary AS
SELECT
    r.Year,
    r.Race_Name,
    r.Date AS Race_Date,
    c.Circuit_Name,
    c.Location AS Circuit_Location,
    c.Country AS Circuit_Country,
    d.FirstName,
    d.LastName,
    d.Nationality,
    drs.total_points AS Driver_Season_Points,
    drs.total_points AS Season_Points,
    dw.wins AS Season_Wins,
    avgfa.avg_lap_time_ms AS Avg_Lap_Time_MS,
    res.final_position AS Final_Position,
    q.qualifying_position AS Qualifying_Pos,
    res.points AS Race_Points,
    pit.Duration AS Pit_Duration,
    pit.stop AS Num_Pit_Stops,
    frl.avg_lap_time_ms AS gold_fastest_avg_lap_by_driver,
    ot.total_overtakes AS Overtakes_In_Race,
    t.Constructor_Name AS Constructor_Team
FROM dltu11.silver_f1.results_silver res
JOIN dltu11.silver_f1.races_silver r      ON res.race_id = r.Race_ID
JOIN dltu11.silver_f1.circuits_silver c   ON r.Circuit_ID = c.Circuit_ID
JOIN dltu11.silver_f1.drivers_silver d    ON res.driver_id = d.DriverID
JOIN dltu11.silver_f1.constructors_silver t ON res.constructor_id = t.Constructor_Id
LEFT JOIN dltu11.silver_f1.gold_driver_standings drs       ON res.driver_id = drs.Driver_ID AND r.Year = drs.Year
LEFT JOIN dltu11.silver_f1.gold_top_drivers_wins dw        ON res.driver_id = dw.Driver_ID
LEFT JOIN dltu11.silver_f1.gold_fastest_avg_lap_by_driver avgfa      ON res.driver_id = avgfa.Driver_ID AND r.Year = avgfa.Year
LEFT JOIN dltu11.silver_f1.gold_fastest_avg_lap_by_driver frl      ON res.driver_id = frl.Driver_ID
LEFT JOIN dltu11.silver_f1.pit_stops_silver pit         ON res.race_id = pit.Race_ID AND res.driver_id = pit.Driver_ID
LEFT JOIN dltu11.silver_f1.qualifying_silver q   ON res.race_id = q.race_id AND res.driver_id = q.driver_id
LEFT JOIN dltu11.silver_f1.gold_overtake_races ot          ON r.Race_ID = ot.Race_ID;

-- COMMAND ----------

