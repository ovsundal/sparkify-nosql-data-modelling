# sparkify-nosql-data-modelling

This is project 1/5 of Udacitys Data Engineering Nanodegree. In this project a database for storing music and artist records are created. Data is then extracted from the source, transformed using Pandas DataFrame, and loaded into the database. Two sets of data is used in the ETL process; song and log data. Song data provides song and artist information, while Log data is more extensive; providing covers song, artist and some metadata about each song. Log data is more extensive, providing artist and artist metadata.

### Prerequisites for running the program
Prerequisites for running the project is python 3.x and cassandra installed

Starting the program
Execute "create_tables.py". This will create a fresh instance of the sparkifydb with empty tables.
Execute "etl.py". This will create a csv file with data, and load it into the tables
