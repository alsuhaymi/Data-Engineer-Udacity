Introduction
Sparkify is a startup, they wants to analyze the data they are collecting on songs on their new music streaming app. The team want to understand the songs that users are listening to 


Prerequisite:

Make sure postgreSQL and python are installed.

Then install psycopg2:
* pip install psycopg2

* Run create_tables.py in order to create the database and the tables:
    * python create_tables.py
* Run etl.py in order to execute the ETL program:
    * python etl.py



Schema Design:

Table Structure 

Fact Table
Table songplays
COLUMN	TYPE
songplay_id	int
start_time	timestamp
user_id	int 
level	varchar 
song_id	varchar 
artist_id	varchar
session_id	int 
location	varchar
user_agent	varchar


Dimensions Tables

Table users

COLUMN	TYPE
user_id	int  
first_name	varchar
last_name	varchar
gender	varchar
level	varchar


Table songs

COLUMN	TYPE
song_id	varchar
title	varchar
artist_id	varchar
year	varchar
duration	int

Table artists

COLUMN	TYPE
artist_id	varchar
name	varchar
location	varchar
latitude	numeric
longitude	numeric

Table time

COLUMN	TYPE
start_time	timestamp
hour	int
day	int
week	int
month	int
year	int
weekday	varchar

