create or replace database sparkifyETL_DB;
use sparkifyETL_DB;
create or replace schema sparkify_schema;
use schema sparkify_schema;


USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::747771850036:role/sparkify_role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://sparkifyetlproject ');

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

-- valdating integration
DESC INTEGRATION S3_INTEGRATION;

-- creating stage
CREATE OR REPLACE STAGE song_stage
URL='s3://sparkifyetlproject/Song_Data/'
STORAGE_INTEGRATION = s3_integration
file_format = JSON_FORMAT;

list @song_stage

SELECT 
    $1 AS raw_data
FROM @song_stage
LIMIT 5;



CREATE OR REPLACE STAGE log_stage
URL='s3://sparkifyetlproject/Log_Data/'
STORAGE_INTEGRATION = s3_integration
file_format = JSON_FORMAT;

list @log_stage


-- Temporary table for raw json

CREATE OR REPLACE TABLE staging_songs_raw (raw VARIANT);

COPY INTO staging_songs_raw
FROM @song_stage
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

SELECT raw FROM staging_songs_raw LIMIT 5;


CREATE OR REPLACE TABLE staging_songs AS
SELECT
    raw:"song_id"::STRING AS song_id,
    raw:"title"::STRING AS title,
    raw:"artist_id"::STRING AS artist_id,
    raw:"artist_name"::STRING AS artist_name,
    raw:"artist_location"::STRING AS artist_location,
    raw:"artist_latitude"::FLOAT AS artist_latitude,
    raw:"artist_longitude"::FLOAT AS artist_longitude,
    raw:"year"::INT AS year,
    raw:"num_songs"::INT AS num_songs
FROM staging_songs_raw;

select * from staging_songs

CREATE OR REPLACE TABLE songs_clean AS
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    artist_name,
    NULLIF(artist_location, '') AS artist_location,
    artist_latitude,
    artist_longitude,
    year,
    num_songs
FROM staging_songs
WHERE song_id IS NOT NULL
  AND title IS NOT NULL;

select * from songs_clean




CREATE OR REPLACE TABLE staging_events_raw (
    json_data VARIANT
);

COPY INTO staging_events_raw
FROM @log_stage
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

SELECT * FROM staging_events_raw LIMIT 5;

CREATE OR REPLACE TABLE staging_events AS
SELECT
    json_data:"artist"::STRING AS artist,
    json_data:"auth"::STRING AS auth,
    json_data:"firstName"::STRING AS first_name,
    json_data:"gender"::STRING AS gender,
    json_data:"itemInSession"::INT AS item_in_session,
    json_data:"lastName"::STRING AS last_name,
    json_data:"length"::FLOAT AS length,
    json_data:"level"::STRING AS level,
    json_data:"location"::STRING AS location,
    json_data:"method"::STRING AS method,
    json_data:"page"::STRING AS page,
    json_data:"registration"::FLOAT AS registration,
    json_data:"sessionId"::INT AS session_id,
    json_data:"song"::STRING AS song,
    json_data:"status"::INT AS status,
    json_data:"ts"::BIGINT AS ts,
    json_data:"userAgent"::STRING AS user_agent,
    json_data:"userId"::STRING AS user_id
FROM staging_events_raw;

select * from staging_events;

-- clean the data
CREATE OR REPLACE TABLE clean_staging_events AS
SELECT DISTINCT
    user_id::INT AS user_id,
    first_name,
    last_name,
    gender,
    level,
    location,
    user_agent,
    session_id,
    page,
    method,
    status,
    ts,
    TO_TIMESTAMP_NTZ(ts / 1000) AS start_time,
    song,
    artist,
    length
FROM staging_events
WHERE page = 'NextSong'
  AND song IS NOT NULL
  AND artist IS NOT NULL
  AND user_id IS NOT NULL;


select * from clean_staging_events
-- star schema table

CREATE OR REPLACE TABLE dim_songs AS
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
FROM songs_clean
WHERE song_id IS NOT NULL;

select * from dim_songs

CREATE OR REPLACE TABLE dim_artists AS
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;

select * from dim_artists

CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
    USER_ID AS user_id,
    FIRST_NAME AS first_name,
    LAST_NAME AS last_name,
    gender,
    level
FROM staging_events
WHERE USER_ID IS NOT NULL;

select * from dim_users

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    TO_TIMESTAMP_LTZ(ts / 1000) AS start_time,
    EXTRACT(hour FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS hour,
    EXTRACT(day FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS day,
    EXTRACT(week FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS week,
    EXTRACT(month FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS month,
    EXTRACT(year FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS year,
    EXTRACT(weekday FROM TO_TIMESTAMP_LTZ(ts / 1000)) AS weekday
FROM staging_events
WHERE ts IS NOT NULL;

select * from dim_time



CREATE OR REPLACE TABLE fact_songplays AS
SELECT
  e.start_time,
  e.user_id,
  e.level,
  COALESCE(s.song_id, 'UNKNOWN') AS song_id,
  COALESCE(s.artist_id, 'UNKNOWN') AS artist_id,
  e.session_id,
  e.location,
  e.user_agent,
  e.song                    AS event_song,
  e.artist                  AS event_artist,
  e.length                  AS event_length
FROM clean_staging_events e
LEFT JOIN songs_clean s
  ON LOWER(TRIM(s.artist_name)) = LOWER(TRIM(e.artist))
     AND (
          (s.year IS NOT NULL AND s.year > 0)
          OR ABS(s.num_songs - 1) < 1
          OR LOWER(TRIM(s.title)) LIKE CONCAT('%', LOWER(TRIM(e.song)), '%')
     )
WHERE e.page = 'NextSong';

select * from fact_songplays where song_id is not null;

SELECT COUNT(*) FROM dim_users;
SELECT COUNT(*) FROM dim_songs;
SELECT COUNT(*) FROM dim_artists;
SELECT COUNT(*) FROM dim_time;
SELECT COUNT(*) FROM fact_songplays;




