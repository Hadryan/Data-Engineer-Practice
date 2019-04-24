# define the SQL statements, which will be imported into the 
# create_table.py and etl.py files

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE STAGING TABLES


staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist TEXT, \
                                 auth            TEXT, \
                                 firstName       TEXT, \
                                 gender          TEXT, \
                                 itemInSession   INT, \
                                 lastName        TEXT, \
                                 length          FLOAT, \
                                 level           TEXT, \
                                 location        TEXT, \
                                 method          TEXT, \
                                 page            TEXT, \
                                 registration    FLOAT, \
                                 sessionId       INT, \
                                 song            TEXT, \
                                 status          INT, \
                                 ts              INT, \
                                 userAgent       TEXT, \
                                 userId          TEXT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT, \
                                 artist_id TEXT, \
                                 artist_latitude  FLOAT, \
                                 artist_location  TEXT, \
                                 artist_name      TEXT, \
                                 song_id          TEXT, \
                                 title            TEXT, \
                                 duration         FLOAT, \
                                 year             INT);
""")

# fact table fact_songplay
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplay (songplay_id INT PRIMARY KEY, \
                            start_time      DATE NOT NULL, \
                            user_id         TEXT NOT NULL, \
                            level           TEXT NOT NULL, \
                            song_id         TEXT NOT NULL, \
                            artist_id       TEXT NOT NULL, \
                            session_id      INT, \
                            location        TEXT, \
                            user_agent      TEXT);
""")

# dimension tables, dim_users, dim_songs, dim_artists and dim_time

user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users (user_id TEXT PRIMARY KEY, \
                        first_name TEXT, \
                        last_name  TEXT, \
                        gender     TEXT, \
                        level      TEXT);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs (song_id TEXT PRIMARY KEY, \
                        title       TEXT, \
                        artist_id   TEXT NOT NULL, \
                        year        INT, \
                        duration    FLOAT);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artists (artist_id TEXT PRIMARY KEY, \
                          name      TEXT NOT NULL, \
                          location  TEXT, \
                          latitude  FLOAT, \
                          longitude FLOAT);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time (start_time DATE PRIMARY KEY, \
                        hour      INT, \
                        day       INT, \
                        week      INT, \
                        month     INT, \
                        year      INT, \
                        weekday   INT);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
                          credentials 'aws_iam_role={}' 
                          compupdate off region 'us-west-2' FORMAT AS JSON {} 
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                       """).format(config['S3'].get('LOG_DATA'),
                                   config['IAM_ROLE'].get('ARN').strip("'"),
                                   config['S3'].get('LOG_JSONPATH'))


staging_songs_copy = ("""copy staging_songs from {} 
                          credentials 'aws_iam_role={}' 
                          compupdate off region 'us-west-2' 
                          FORMAT AS JSON 'auto' 
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                        """).format(config['S3'].get('SONG_DATA'),
                                    config['IAM_ROLE'].get('ARN').strip("'"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, \
                                    se.userId as user_id, \
                                    se.level as level, \
                                    ss.song_id as song_id, \
                                    ss.artist_id as artist_id, \
                                    se.sessionId as session_id, 
                                    se.location as location, \
                                    se.userAgent as user_agent \
                            FROM staging_events as se \
                            JOIN staging_songs as ss ON (se.song = ss.title) \
                            WHERE se.page = 'NextSong';
                                          
""")

user_table_insert = ("""INSERT INTO dim_users (user_id, first_name, last_name, gender, level) \
                            SELECT DISTINCT userId as user_id, \
                                            firstName as first_name, \
                                            lastName as last_name, \
                                            gender as gender, \
                                            level as level \
                             FROM staging_events \
                             WHERE page='NextSong';
""")

song_table_insert = ("""INSERT INTO dim_songs (song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT song_id as song_id, \
                                        title as title, \
                                        artist_id as artist_id, \
                                        year as year, \
                                        duration as duration \
                         FROM staging_songs \
                         WHERE artist_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO dim_artists (artist_id, name, location, latitude, longitude) \
                          SELECT DISTINCT artist_id as artist_id, \
                                          artist_name as name, \
                                          artist_location as location, \
                                          artist_latitude as latitude, \
                                          artist_longitude as longitude \
                          FROM staging_songs \
                          WHERE artist_name IS NOT NULL;
""")


# since we're dealing with millisecond epochs we're going to either divide or multiple the epoch value
# by 1000.
time_table_insert = ("""INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday) \
                        SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, \
                               extract(hour from start_time) as hour, \
                               extract(day from start_time) as day, \
                               extract(week from start_time) as week, \
                               extract(month from start_time) as month, \
                               extract(year from start_time) as year, \
                               extract(weekday from start_time) as weekday \
                         FROM staging_events \
                         WHERE page='NextSong';
                              
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]
