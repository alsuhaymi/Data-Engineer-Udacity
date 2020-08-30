 
   
  ## Introduction:
Sparkify is a startup, they wants to analyze the data they are collecting on songs on their new music streaming app. The team want to understand the songs that users are listening to 


## Prerequisite:

*  Make sure python are installed.

* Then install psycopg2:
```sh
$pip install pyspark
```
* fill the dl.cfg file with the required information to get the data from s3 amazon.

* Run etl.py in order to execute the ETL program:
```sh
$python etl.py
``` 

Schema Design:

# Tables Structure 

## Fact Table
##### Songplays Table 


| COLUMN |  TYPE |
| ------ | ------ |
|songplay_id|int|
|start_time|timestamp|
|user_id|int|
|level|varchar |
|song_id|varchar| 
|artist_id|varchar|
|session_id|int |
|location|varchar|
|user_agent|varchar|


## Dimensions Tables

#### Users Table 
| COLUMN |  TYPE |
| ------ | ------ |
|first_name|	varchar|
|last_name|	varchar|
|gender|	varchar|
|level|	varchar|


#### Songs Table 

| COLUMN |  TYPE |
| ------ | ------ |
|song_id|	varchar|
|title|	varchar|
|artist_id|	varchar|
|year|	varchar|
|duration|	int|

#### Artists Table 

| COLUMN |  TYPE |
| ------ | ------ |
|artist_id|	varchar|
|name|	varchar|
|location|	varchar|
|latitude|	numeric|
|longitude|	numeric|

#### Time Table 

| COLUMN |  TYPE |
| ------ | ------ |
|start_time|	timestamp|
|hour|	int|
|day|	int|
|week|	int|
|month|	int|
|year|	int|
|weekday|	varchar|
