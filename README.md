# Sparkify Songplay Database



## Goals



## DB Schema Design

### Create Tables
...

```python
songplay_table_create = ("""  \
    CREATE TABLE IF NOT EXISTS songplays (  \
        songplay_id SERIAL PRIMARY KEY,  \
        start_time TIMESTAMP NOT NULL,  \
        user_id INT NOT NULL,  \
        level VARCHAR NOT NULL,  \
        song_id VARCHAR,  \
        artist_id VARCHAR,  \
        session_id INT,  \
        location VARCHAR,  \
        user_agent VARCHAR,  \
        UNIQUE (start_time, user_id)  \
    );  \
""")
```
...

```python
user_table_create = ("""  \
    CREATE TABLE IF NOT EXISTS users (  \
        user_id INT PRIMARY KEY,  \
        first_name VARCHAR NOT NULL,  \
        last_name VARCHAR NOT NULL,  \
        gender VARCHAR,  \
        level VARCHAR NOT NULL  \
    );  \
""")
```
...

```python
song_table_create = ("""  \
    CREATE TABLE IF NOT EXISTS songs (  \
        song_id VARCHAR PRIMARY KEY,  \
        title VARCHAR NOT NULL,  \
        artist_id VARCHAR NOT NULL,  \
        year INT,  \
        duration DOUBLE PRECISION  \
    );  \
""")
```
...

```python
artist_table_create = ("""  \
    CREATE TABLE IF NOT EXISTS artists (  \
        artist_id VARCHAR PRIMARY KEY,  \
        name VARCHAR NOT NULL,  \
        location VARCHAR,  \
        latitude DOUBLE PRECISION,  \
        longitude DOUBLE PRECISION  \
    );  \
""")
```
...

```python
time_table_create = ("""  \
    CREATE TABLE IF NOT EXISTS time (  \
        start_time TIMESTAMP PRIMARY KEY,  \
        hour SMALLINT NOT NULL,  \
        day SMALLINT NOT NULL,  \
        week SMALLINT NOT NULL,  \
        month SMALLINT NOT NULL,  \
        year SMALLINT NOT NULL,  \
        weekday SMALLINT NOT NULL  \
    );  \
""")
```
...

### Insert Records
...

```python
songplay_table_insert = ("""  \
    INSERT INTO songplays (  \
        start_time, user_id, level, song_id, artist_id,  \
        session_id, location, user_agent  \
    )  \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)  \
    ON CONFLICT (start_time, user_id)  \
    DO UPDATE SET  \
        level = EXCLUDED.level,  \
        song_id = EXCLUDED.song_id,  \
        artist_id = EXCLUDED.artist_id,  \
        session_id = EXCLUDED.session_id,  \
        location = EXCLUDED.location,  \
        user_agent = EXCLUDED.user_agent  \
""")
```
...

```python
user_table_insert = ("""  \
    INSERT INTO users (  \
        user_id, first_name, last_name, gender, level  \
    )  \
    VALUES (%s, %s, %s, %s, %s)  \
    ON CONFLICT (user_id)  \
    DO UPDATE SET  \
        first_name = EXCLUDED.first_name,  \
        last_name = EXCLUDED.last_name,  \
        gender = EXCLUDED.gender,  \
        level = EXCLUDED.level  \
""")
```
...

```python
song_table_insert = ("""  \
    INSERT INTO songs (  \
        song_id, title, artist_id, year, duration  \
    )  \
    VALUES (%s, %s, %s, %s, %s)  \
    ON CONFLICT (song_id)  \
    DO UPDATE SET  \
        title = EXCLUDED.title,  \
        artist_id = EXCLUDED.artist_id,  \
        year = EXCLUDED.year,  \
        duration = EXCLUDED.duration  \
""")
```
...

```python
artist_table_insert = ("""  \
    INSERT INTO artists (  \
        artist_id, name, location, latitude, longitude  \
    )  \
    VALUES (%s, %s, %s, %s, %s)  \
    ON CONFLICT (artist_id)  \
    DO UPDATE SET  \
        name = EXCLUDED.name,  \
        location = EXCLUDED.location,  \
        latitude = EXCLUDED.latitude,  \
        longitude = EXCLUDED.longitude  \
""")
```
...

```python
time_table_insert = ("""  \
    INSERT INTO time (  \
        start_time, hour, day, week, month, year, weekday  \
    )  \
    VALUES (%s, %s, %s, %s, %s, %s, %s)  \
    ON CONFLICT (start_time)  \
    DO NOTHING  \
""")
```
...

### Find Songs
...

```python
song_select = ("""  \
    SELECT s.song_id, a.artist_id  \
    FROM   songs s  \
    JOIN   artists a  \
    ON     s.artist_id = a.artist_id  \
    WHERE  s.title = %s AND a.name = %s AND s.duration = %s  \
""")
```
...

## ETL Pipeline



## Example Queries

Some examples of queries:

```sql
SELECT a.name, s.title
FROM   songplays p
JOIN   artists a
ON     p.artist_id = a.artist_id
JOIN   songs s
ON     p.song_id = s.song_id
WHERE  p.song_id <> 'None' AND p.artist_id <> 'None'
ORDER  BY a.name
```