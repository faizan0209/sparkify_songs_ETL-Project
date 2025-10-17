# Sparkify Data Warehousing Project

> **Note:** This is part of the **SMIT Cloud Data Engineering (Batch 2)** program.

---

## Project Overview

This project focuses on building a **scalable data warehouse** using **Snowflake** for Sparkify, a fictional music streaming startup. The goal is to create a **star schema**, perform **ETL operations** on song and user activity data, and enable efficient analytical queries.

Key goals:  
- Extract JSON data from **S3 buckets**  
- Transform and clean the data  
- Load data into **fact and dimension tables** in Snowflake  
- Enable queries for analytics and dashboard visualization

---


## Datasets Description

### 1. Song Data
Metadata about each song and its artist.

**Example:**
```json
{
  "num_songs": 1,
  "artist_id": "ARJIE2Y1187B994AB7",
  "artist_name": "Line Renaud",
  "song_id": "SOUPIRU12A6D4FA1E1",
  "title": "Der Kleine Dompfaff",
  "duration": 152.92,
  "year": 0
}
```

**Key Fields:**
- `song_id` – Unique identifier for the song  
- `title` – Song title  
- `artist_id` – Artist reference  
- `artist_name` – Artist name  
- `year` – Release year  
- `duration` – Duration in seconds

### 2. Log Data (Event Data)
Tracks user activity in Sparkify.

**Example:**
```json
{
  "artist": "Pavement",
  "firstName": "Sylvie",
  "lastName": "Cruz",
  "gender": "F",
  "level": "free",
  "song": "Mercy:The Laundromat",
  "sessionId": 345,
  "ts": 1541990258796,
  "userId": "10"
}
```

**Key Fields:**
- `userId` – Unique user ID  
- `firstName`, `lastName`, `gender` – User details  
- `level` – Subscription type (free/paid)  
- `song`, `artist`, `length` – Song played  
- `sessionId` – User session  
- `location` – User location  
- `ts` – Timestamp

---

## Data Model (Star Schema)

**Fact Table:**  
- `fact_songplays` – Records of song plays

**Dimension Tables:**  
- `dim_users` – User details  
- `dim_songs` – Song metadata  
- `dim_artists` – Artist metadata  
- `dim_time` – Timestamp breakdown

> Fact table is linked to dimensions for **analytics queries**.

---

## ETL Workflow

1. **Extract:** Read JSON data from **S3 buckets**  
2. **Transform:** Clean and structure data into staging tables, remove duplicates and NULLs  
3. **Load:** Insert into **fact and dimension tables** in Snowflake

---

## Key Achievements

- Successfully loaded **6,820 events** into the fact table  
- Cleaned and transformed **song and log data**  
- Designed a **star schema** optimized for queries  
- ETL pipeline integrated with **S3 + Snowflake**

---

## Tech Stack

- **Database:** Snowflake  
- **Data Storage:** AWS S3  
- **Querying & ETL:** SQL (Snowflake)  
- **Version Control:** Git & GitHub

---

## Next Steps

- Build **dashboard** to visualize top songs, user activity, and song popularity  
- Implement **additional metrics** like revenue or subscription trends

---

## Learning Outcomes

- Hands-on experience with **cloud data warehousing**  
- Learned **Snowflake stages, file formats, and storage integrations**  
- Built **ETL pipelines** and **fact/dimension tables**  
- Prepared for **real-world analytics and dashboarding**

