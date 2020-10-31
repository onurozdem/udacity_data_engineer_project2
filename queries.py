#DROP TABLES QUERIES
drop_table_1 = "DROP TABLE IF EXISTS music_app_history1"
drop_table_2_3 = "DROP TABLE IF EXISTS music_app_history2"

# DROP KEYSPACE
drop_keyspace = "DROP KEYSPACE IF EXISTS sparkify"

# CREATE KEYSPACE
create_keyspace = """ CREATE KEYSPACE IF NOT EXISTS sparkify
                        WITH REPLICATION = 
                        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""

#CREATE TABLES QUERIES
create_query_1 = "CREATE TABLE IF NOT EXISTS music_app_history1 "
create_query_1 = create_query_1 + """(artist text, 
                                      item_in_session int, 
                                      length double, 
                                      session_id int, 
                                      song_title text,
                                      PRIMARY KEY (session_id, item_in_session))"""

create_query_2_3 = "CREATE TABLE IF NOT EXISTS music_app_history2 "
create_query_2_3 = create_query_2_3 + """(artist text, 
                                          first_name text, 
                                          last_name text,
                                          item_in_session int,
                                          session_id int, 
                                          song_title text, 
                                          user_id int, 
                                          PRIMARY KEY ((user_id, session_id), item_in_session))"""


# INSERT QUERIES
insert_query_1 = """INSERT INTO music_app_history1 (artist,  
                                                    item_in_session, 
                                                    length, 
                                                    session_id, 
                                                    song_title)"""
insert_query_1 = insert_query_1 + " VALUES (%s, %s, %s, %s, %s)"

insert_query_2_3 = """INSERT INTO music_app_history2 (artist, 
                                                      first_name, 
                                                      last_name, 
                                                      item_in_session,
                                                      session_id, 
                                                      song_title, 
                                                      user_id)"""
insert_query_2_3 = insert_query_2_3 + " VALUES (%s, %s, %s, %s, %s, %s, %s)"


# SELECT QUERIES

#1. Give me the artist, song title and song's length in the music app history 
#     that was heard during sessionId = 338, and itemInSession = 4
select_query_1 = """SELECT artist, 
                           song_title, 
                           length 
                      FROM music_app_history1 
                      WHERE session_id = 338 AND
                            item_in_session = 4"""

#2. Give me only the following: name of artist, song (sorted by itemInSession) 
#      and user (first and last name) for userid = 10, sessionid = 182
select_query_2 = """SELECT artist, 
                           song_title, 
                           first_name, 
                           last_name 
                      FROM music_app_history2 
                      WHERE user_id = 10 AND 
                            session_id = 182"""

#3. Give me every user name (first and last) in my music app history 
#      who listened to the song 'All Hands Against His Own'
select_query_3 = """SELECT first_name, 
                           last_name 
                      FROM music_app_history2 
                      WHERE song_title = 'All Hands Against His Own' 
                      ALLOW FILTERING"""


# QUERY LISTS
create_table_queries = [create_query_1, create_query_2_3]
drop_table_queries = [drop_table_1, drop_table_2_3]