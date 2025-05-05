-- Force fetch task to avoid MapReduce
SET hive.fetch.task.conversion=more;
SET hive.exec.mode.local.auto=true;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.reduce.memory.mb=1024;

-- Create database
DROP DATABASE IF EXISTS stream_analytics CASCADE;
CREATE DATABASE stream_analytics;
USE stream_analytics;

-- Raw Logs Table (External)
CREATE EXTERNAL TABLE raw_logs (
  user_id INT,
  content_id INT,
  action STRING,
  time_stamp STRING,
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Raw Metadata Table (External)
CREATE EXTERNAL TABLE raw_metadata (
  content_id INT,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Add partitions
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='01') LOCATION '/raw/logs/2025/03/01';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='02') LOCATION '/raw/logs/2025/03/02';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='03') LOCATION '/raw/logs/2025/03/03';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='04') LOCATION '/raw/logs/2025/03/04';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='05') LOCATION '/raw/logs/2025/03/05';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='06') LOCATION '/raw/logs/2025/03/06';
ALTER TABLE raw_logs ADD PARTITION (year='2025', month='03', day='07') LOCATION '/raw/logs/2025/03/07';

ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='01') LOCATION '/raw/metadata/2025/03/01';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='02') LOCATION '/raw/metadata/2025/03/02';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='03') LOCATION '/raw/metadata/2025/03/03';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='04') LOCATION '/raw/metadata/2025/03/04';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='05') LOCATION '/raw/metadata/2025/03/05';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='06') LOCATION '/raw/metadata/2025/03/06';
ALTER TABLE raw_metadata ADD PARTITION (year='2025', month='03', day='07') LOCATION '/raw/metadata/2025/03/07';

-- Fact Table (TEXTFILE)
CREATE TABLE fact_user_actions (
  user_id INT,
  content_id INT,
  action STRING,
  time_stamp STRING,
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS TEXTFILE;

-- Dimension Table (TEXTFILE)
CREATE TABLE dim_content (
  content_id INT,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
STORED AS TEXTFILE;

-- Transform Data
INSERT OVERWRITE TABLE fact_user_actions
PARTITION (year='2025', month='03', day='01')
SELECT user_id, content_id, action, time_stamp, device, region, session_id
FROM raw_logs
WHERE year = '2025' AND month = '03' AND day = '01';

INSERT OVERWRITE TABLE dim_content
SELECT DISTINCT content_id, title, category, length, artist
FROM raw_metadata
WHERE year = '2025' AND month = '03' AND day = '01';

-- Load more days into fact_user_actions for queries
INSERT OVERWRITE TABLE fact_user_actions
PARTITION (year='2025', month='03', day='02')
SELECT user_id, content_id, action, time_stamp, device, region, session_id
FROM raw_logs
WHERE year = '2025' AND month = '03' AND day = '02';

INSERT OVERWRITE TABLE fact_user_actions
PARTITION (year='2025', month='03', day='03')
SELECT user_id, content_id, action, time_stamp, device, region, session_id
FROM raw_logs
WHERE year = '2025' AND month = '03' AND day = '03';

-- Queries
SELECT region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
WHERE year = '2025' AND month = '03'
GROUP BY region;

SELECT d.category, COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
WHERE f.action = 'play' AND f.year = '2025' AND f.month = '03'
GROUP BY d.category
ORDER BY play_count DESC
LIMIT 5;

WITH session_times AS (
  SELECT session_id, MIN(time_stamp) AS start_time, MAX(time_stamp) AS end_time
  FROM fact_user_actions
  WHERE year = '2025' AND month = '03'
  GROUP BY session_id
)
SELECT AVG(UNIX_TIMESTAMP(end_time, 'yyyy-MM-dd HH:mm:ss') - UNIX_TIMESTAMP(start_time, 'yyyy-MM-dd HH:mm:ss')) / 60 AS avg_session_mins
FROM session_times;
