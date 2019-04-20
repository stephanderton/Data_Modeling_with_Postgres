# Data Modeling with Postgres

>
> **Stephanie Anderton**
> DEND Project #1
> April 20, 2019
>

## Sparkify Songplay Database

The purpose of this database is to make it easier for Sparkify to query their data, with the main goal being to understand what songs users are listening to. It is populated with data extracted from JSON metadata on songs and JSON logs on user activity in the Sparkify music streaming app.

## Datasets

### Song Dataset

The song dataset consists of files in JSON format, each containing metadata about a single song and the artist of that song. The files are partitioned in subdirectories, organized by the three letters after `TR` of each song's track ID. For example, these are the file paths for two files in this dataset.

```
song_data/A/B/C/TRABCAJ12903CDFCC2.json
song_data/A/B/A/TRABAVQ12903CBF7E0.json
```

Here is an example of what a single song file, TRABCAJ12903CDFCC2.json, looks like in JSON format.

```json
{"num_songs": 1, "artist_id": "ARULZCI1241B9C8611", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Luna Orbit Project", "song_id": "SOSWKAV12AB018FC91", "title": "Midnight Star", "duration": 335.51628, "year": 0}
```

### Log Dataset

The log dataset consists of files in JSON format, each containing metadata about event activity in the music streaming app. These files are partitioned in subdirectories, organized by year and month. For example, these two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Here is an example of what the first line of data (a single event record in JSON format) looks like in the file labelled 2018-11-23-events.json.

```json
{"artist":"Great Lake Swimmers","auth":"Logged In","firstName":"Kevin","gender":"M","itemInSession":0,"lastName":"Arellano","length":215.11791,"level":"free","location":"Harrisburg-Carlisle, PA","method":"PUT","page":"NextSong","registration":1540006905796.0,"sessionId":815,"song":"Your Rocky Spine","status":200,"ts":1542931645796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.125 Safari\/537.36\"","userId":"66"}
...
```

**Note**: Only log records with `page` feature as “NextSong” are associated with song plays and loaded to the database.

## DB Schema

The **sparkifydb** data model is essentially a star schema (or a minimal snowflake schema because of the relationship between songs and artists), and is implemented in a Postgres database. It contains one fact table of songplays and four dimension tables for users, songs, artists, and time.


<img src="data_model.png" width=600>



This schema is not fully normalized, as the `level` feature is replicated in the songplays fact table as well as the users dimension table. It’s structure will allow queries to be optimized for song play analysis, with simpler joins and aggregations. All essential data for songplays and user level is contained in the songplays fact table.

## ETL Pipeline

The ETL pipeline extracts data from files in two local directories, /data/log_data and /data/song_data, and then transforms the data and loads into the five tables of the **sparkifydb** database. This is handled by three files using Python and SQL.

| Step  | File             | Purpose                                                      |
| ----- | ---------------- | ------------------------------------------------------------ |
| **1** | create_tables.py | Creates and initializes the tables for the **sparkifydb** database. |
| **2** | etl.py           | Reads and processes files from the song_data and log_data directories, and loads them into the **sparkifydb** database tables. |
| -     | sql_queries.py   | Contains all SQL queries. This file is imported into etl.py. |

**Note**: These three Python files are found in the same directory that contains the data directory, which itself contains the song_data and log_data directories that are to be processed.

## Steps to Run the ETL

1. In a terminal, run **python create_tables.py** to reset the tables in the **sparkifydb** database:

```bash
oot@68edcb7a4e06:/home/workspace# python create_tables.py
Connected to default database
Dropped the sparkifydb database
Created the sparkifydb database
Connected to sparkifydb database
root@68edcb7a4e06:/home/workspace#
```

2. Then, in the (same) terminal, run **python etl.py** to process the datasets: (*Note: some output lines in the example output below have been removed for readability* )

```bash
root@68edcb7a4e06:/home/workspace# python etl.py
Connected the sparkifydb database
71 files found in data/song_data
1/71 files processed.
2/71 files processed.
3/71 files processed.
...
68/71 files processed.
69/71 files processed.
70/71 files processed.
71/71 files processed.
30 files found in data/log_data
1/30 files processed.
2/30 files processed.
3/30 files processed.
...
28/30 files processed.
29/30 files processed.
30/30 files processed.
root@68edcb7a4e06:/home/workspace#
```

## Sample Query

**Note**: The following query was run in a Jupyter Notebook, but the python code needed to run the SQL statement has not been included for readability. This query could be run as is from pgAdmin4 or a PSQL terminal.

```sql
SELECT DISTINCT p.user_id, p.song_id, p.artist_id
FROM   songplays p
WHERE  p.level = 'paid';
```

Output:

```sql
 * postgresql://student:***@127.0.0.1/sparkifydb
23 rows affected.
user_id	song_id	artist_id
70	None	None
85	None	None
82	None	None
25	None	None
58	None	None
36	None	None
15	None	None
88	None	None
42	None	None
80	None	None
30	None	None
73	None	None
15	SOZCTXZ12AB0182364	AR5KOSW1187FB35FF4
95	None	None
16	None	None
29	None	None
97	None	None
72	None	None
65	None	None
20	None	None
49	None	None
44	None	None
24	None	None
```

