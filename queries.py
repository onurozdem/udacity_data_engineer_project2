#DROP TABLES QUERIES
drop_song_info_by_session = "DROP TABLE IF EXISTS song_info_by_session"
drop_song_info_by_user = "DROP TABLE IF EXISTS song_info_by_user"
drop_user_info_by_song = "DROP TABLE IF EXISTS user_info_by_song"

# DROP KEYSPACE
drop_keyspace = "DROP KEYSPACE IF EXISTS sparkify"

# CREATE KEYSPACE
create_keyspace = """ CREATE KEYSPACE IF NOT EXISTS sparkify
                        WITH REPLICATION = 
                        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""

#CREATE TABLES QUERIES
create_song_info_by_session = "CREATE TABLE IF NOT EXISTS song_info_by_session "
create_song_info_by_session = create_song_info_by_session + """(session_id int, 
                                                                item_in_session int, 
                                                                artist text, 
                                                                length double, 
                                                                song_title text,
                                                                PRIMARY KEY (session_id, item_in_session))"""

create_song_info_by_user = "CREATE TABLE IF NOT EXISTS song_info_by_user "
create_song_info_by_user = create_song_info_by_user + """(user_id int,
                                                          session_id int,
                                                          item_in_session int,
                                                          artist text, 
                                                          first_name text, 
                                                          last_name text,
                                                          song_title text, 
                                                          PRIMARY KEY ((user_id, session_id), item_in_session))"""

create_user_info_by_song = "CREATE TABLE IF NOT EXISTS user_info_by_song "
create_user_info_by_song = create_user_info_by_song + """(song_title text, 
                                                          user_id int,
                                                          first_name text, 
                                                          last_name text,
                                                          PRIMARY KEY (song_title, user_id))"""

# INSERT QUERIES
insert_song_info_by_session = """INSERT INTO song_info_by_session (session_id,
                                                                   item_in_session, 
                                                                   artist,  
                                                                   length, 
                                                                   song_title)"""
insert_song_info_by_session = insert_song_info_by_session + " VALUES (%s, %s, %s, %s, %s)"

insert_song_info_by_user = """INSERT INTO song_info_by_user (user_id,
                                                             session_id,
                                                             item_in_session,
                                                             artist, 
                                                             first_name, 
                                                             last_name, 
                                                             song_title)"""
insert_song_info_by_user = insert_song_info_by_user + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

insert_user_info_by_song = """INSERT INTO user_info_by_song (song_title,
                                                             user_id,
                                                             first_name, 
                                                             last_name)"""
insert_user_info_by_song = insert_user_info_by_song + " VALUES (%s, %s, %s, %s)"

# SELECT QUERIES

#1. Give me the artist, song title and song's length in the music app history 
#     that was heard during sessionId = 338, and itemInSession = 4
select_song_info_by_session = """SELECT artist, 
                                        song_title, 
                                        length 
                                   FROM song_info_by_session 
                                   WHERE session_id = 338 AND
                                         item_in_session = 4"""

#2. Give me only the following: name of artist, song (sorted by itemInSession) 
#      and user (first and last name) for userid = 10, sessionid = 182
select_song_info_by_user = """SELECT artist, 
                                     song_title, 
                                     first_name, 
                                     last_name 
                                FROM song_info_by_user 
                                WHERE user_id = 10 AND 
                                      session_id = 182"""

#3. Give me every user name (first and last) in my music app history 
#      who listened to the song 'All Hands Against His Own'
select_user_info_by_song = """SELECT first_name, 
                                     last_name 
                                FROM user_info_by_song 
                                WHERE song_title = 'All Hands Against His Own'"""


# QUERY LISTS
create_table_queries = [create_song_info_by_session, create_song_info_by_user, create_user_info_by_song]
drop_table_queries = [drop_song_info_by_session, drop_song_info_by_user, drop_user_info_by_song]


