create_keyspace_sparkifydb = """
    CREATE KEYSPACE IF NOT EXISTS sparkifydb 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """;

# CREATE TABLES

create_music_library = 'CREATE TABLE IF NOT EXISTS music_library ' \
                '(sessionId int, itemInSession int, artist text, songTitle text, songLength decimal, ' \
                'PRIMARY KEY(sessionId, itemInSession))'

create_artist_library = 'CREATE TABLE IF NOT EXISTS artist_library ' \
                 '(artist text, song_title text, itemInSession int, user_first_name text, user_last_name text, userId int, sessionId int, ' \
                 'PRIMARY KEY(userId, sessionId))'

create_user_library = 'CREATE TABLE IF NOT EXISTS user_library ' \
               '(userId int, sessionId int, user_first_name text, user_last_name text, song_title text, ' \
               'PRIMARY KEY(song_title, userId, sessionId))'

# INSERTS

insert_music_library = "INSERT INTO music_library(sessionId, itemInSession, artist, songTitle, songLength), " \
                       "VALUES(%s, %s, %s, %s, %s) "

insert_artist_library = "INSERT INTO artist_library(artist, song_title, itemInSession, user_first_name, user_last_name, userId, sessionId), " \
                        "VALUES(%s, %s, %s, %s, %s, %s, %s)  "

insert_user_library = "INSERT INTO user_library(userId, sessionId, user_first_name, user_last_name, song_title), " \
                      "VALUES (%s, %s, %s, %s, %s) "

# SELECTS

select_music_library = 'SELECT * from music_library WHERE sessionId = 338 AND itemInSession = 4'

select_artist_library = 'SELECT * from artist_library WHERE userId = 10'

select_user_library = "SELECT user_first_name, user_last_name FROM user_library WHERE song_title='You Gotta Be' AND userId = 8"


# DELETES

drop_music_library = 'DROP TABLE music_library'

drop_artist_library = 'DROP TABLE artist_library'

drop_user_library = 'DROP TABLE user_library'


create_tables = (create_music_library, create_artist_library, create_user_library)
drop_tables = (drop_music_library, drop_artist_library, drop_user_library)
