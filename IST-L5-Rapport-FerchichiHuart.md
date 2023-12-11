---
title: "IST - Lab 05: Serverless data ingestion and processing"
author: "Farouk Ferchichi & Hugo Huart"
format:
    pdf:
        geometry:
            margin=1.5cm
---

# IST - LAB 05 : Serverless data ingestion and processing

**Farouk Ferchichi & Hugo Huart**

# Task 1: Explore MeteoSwiss data

## _For the two data products copy the URLs where the data can be downloaded in the report_

For "Automatic weather stations – Current measurement values" the URL is 
https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv

For "Weather stations of the automatic monitoring network" the URL is
https://data.geo.admin.ch/ch.meteoschweiz.messnetz-automatisch/ch.meteoschweiz.messnetz-automatisch_en.json

## _Document your exploration of the measurement values_

### Nearby weather station

The closest weather station to where I live is the Vevey/Corseaux station. Its 3 letters ID is `VEV`.

### Temperature column

According to the data description provided at the URL https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/info/VQHA80_en.txt,
the column `tre200s0` corresponds to the temperature in °C 2 meters above ground

### Current temperature

The temperature is 9.9°C.

### Comparison with MétéoSuisse

Yes, it corresponds with the temperature shown by MétéoSuisse on the app.

### Date column

It contains the date and time of the measurement with format YYYYMMDDHHmm, in UTC time (not CET).
The minutes are a multiple of 10 minutes (00, 10, 20, etc.)

# Task 2: Upload the current measurement data to S3 and run SQL queries on it

A bucket was created with name `ist-meteo-grh-ferchichi-huart`.

A database with name `meteoswiss_grh` was created.

The `current` table was created, with the additional SerDe option `skip.header.line.count` set to 1 to prevent
the inclusion of original the header in the table.

# Task 3: Write a python script to download the current measurement values from MeteoSwiss and upload them to S3

Here's the Python script used:

```python
import boto3
import requests

BUCKET="ist-meteo-grh-ferchichi-huart"
KEY="current/VQHA80.csv"

CSV_URL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv"

def fetch_csv(url:str=CSV_URL) -> bytes:
    return requests.get(url).content

def upload_to_s3(csv_data:bytes, bucket_name:str, key:str):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, key)
    obj.put(Body=csv_data)

def main():
    csv_data = fetch_csv()
    upload_to_s3(csv_data, BUCKET, KEY)

if __name__ == "__main__":
    main()
```

The script was tested successfully

# Task 4: Convert your script into an AWS Lambda function for data ingestion

The following entities were created:

* A Lambda function named `meteoswiss-ingest-grh`
* An IAM policy named `meteoswiss-grh-write` with `PutObject` permission on the S3 bucket.

The policy was assigned to the Lambda role. Then an .zip upload package was created with the Lambda handler script and the `requests` package.

Here's the handler script code:

```python
import os
import datetime
import boto3
import json
import requests

BUCKET="ist-meteo-grh-ferchichi-huart"
KEY="current/VQHA80.csv"

CSV_URL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv"

def fetch_csv(url:str=CSV_URL) -> bytes:
    return requests.get(url).content

def upload_to_s3(csv_data:bytes, bucket_name:str, key:str):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, key)
    obj.put(Body=csv_data)

def timestamp_key(key:str) -> str:
    now = datetime.datetime.utcnow().isoformat()
    key = os.path.splitext(key)[0]
    return f"{key}-{now}.csv"
    
def main():
    csv_data = fetch_csv()
    key = timestamp_key(KEY)
    upload_to_s3(csv_data, BUCKET, key)

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('')
    }
```

It was successfully tested.

# Task 5: Create an event rule that triggers your function every 10 minutes

A 10-minutes schedule named `MeteoswissIngestGrHFerchichiHuart` was created.

# Task 6: transform the weather stations file into a csv file

## _Examine the YAML. There are two top-level keys, what are their names?_

There are several top level keys, which are `crs`, `license`, `mapname`, `map_long_name`, `map_short_name`, `map_abstract`, `creation_time`,
`type` and finally `features` which is an array.

## _What key contains the station name?_

The `station_name` keys

## Final jq command

Here's the final JQ command:

```bash
jq -j '.features|.[]|.id,",\"",.properties.station_name,"\",",.properties.altitude,",",.geometry.coordinates[0],",",.geometry.coordinates[1],"\n"'
```

# Task 7: Query the accumulated data

## 2. All Payerne measurements

Here are the requested query and its output:

```sql
SELECT * FROM "meteoswiss_grh"."current"
WHERE station = 'PAY'
ORDER BY datetime ASC
```

```
#	station	datetime	temperature	precipitation	sunshine	radiation	humidity	despoint	wind_dir	wind_speed	gust_peak	pressure	press_sea	press_sea_qnh	height_850_hpa	heigh_700_hpa	wind_dir_vec	wind_speed_tower	gust_peak_tower	temp_tool1	humidity_tower	dew_point_tower
1	PAY	202312101330	10.4	0.0	10.0	273.0	59.1	2.8	222.0	16.2	25.6	960.4	1018.3	1018.2								
2	PAY	202312101430	9.6	0.0	0.0	92.0	62.6	2.8	234.0	9.4	15.1	959.5	1017.5	1017.2								
3	PAY	202312101500	9.1	0.0	0.0	49.0	62.7	2.4	234.0	13.7	22.0	959.6	1017.7	1017.3								
4	PAY	202312101720	8.8	0.0	0.0	0.0	66.9	3.0	217.0	17.3	30.2	958.2	1016.3	1015.9								
5	PAY	202312101730	8.7	0.0	0.0	0.0	67.0	2.9	226.0	16.9	29.5	958.1	1016.2	1015.8								
6	PAY	202312101740	8.9	0.0	0.0	0.0	63.9	2.4	221.0	19.1	30.6	957.8	1015.9	1015.5								
7	PAY	202312101750	8.7	0.0	0.0	0.0	65.3	2.6	230.0	17.3	29.9	957.6	1015.7	1015.2								
8	PAY	202312101800	8.8	0.0	0.0	0.0	65.3	2.7	231.0	16.9	26.6	957.8	1015.9	1015.5								
9	PAY	202312101810	8.9	0.0	0.0	0.0	63.3	2.3	239.0	18.0	28.1	957.8	1015.9	1015.5								
10	PAY	202312101820	9.0	0.0	0.0	0.0	62.2	2.2	235.0	15.8	32.0	957.7	1015.7	1015.3								
[truncated]
```

## 3. Hourly Payerne measurements

Here are the requested query and its output:

```sql
SELECT station,
	MAX(temperature) as temperature_max,
	substring(cast(datetime as varchar), 9, 2) AS hour
FROM "meteoswiss_grh"."current"
WHERE station = 'PAY'
GROUP BY station, 3
ORDER BY hour ASC
```

```
#	station	temperature_max	hour
1	PAY	7.4	00
2	PAY	7.4	01
3	PAY	10.4	13
4	PAY	9.6	14
5	PAY	9.1	15
6	PAY	8.9	17
7	PAY	9.1	18
8	PAY	8.8	19
9	PAY	7.9	20
10	PAY	8.1	21
[truncated]
```

## 4. Altitudes of stations 1

A `stations` table was created with the following query:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `meteoswiss_grh`.`stations` (
  `id` varchar(5),
  `station_name` varchar(50),
  `altitude` int,
  `coord_lng` int,
  `coord_lat` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = ',',
  'skip.header.line.count' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ist-meteo-grh-ferchichi-huart/stations_csv/'
TBLPROPERTIES ('classification' = 'csv');
```

Here are the requested query and its output:

```sql
SELECT * FROM "meteoswiss_grh"."stations"
WHERE altitude >= 400 AND altitude < 500
ORDER BY altitude ASC
```

```
#	id	station_name	altitude	coord_lng	coord_lat
1	BEX	"Bex"	402	2565805	1121511
2	BUE	"Bülach"	403	2682029	1263775
3	VEV	"Vevey / Corseaux"	405	2552106	1146847
4	SCM	"Schmerikon"	408	2713726	1231533
5	DOB	"Benken / Doggen"	408	2715388	1227540
6	OBR	"Oberriet / Kriessern"	409	2764171	1249582
7	JON	"Jona"	410	2706761	1231290
8	GVE	"Genève / Cointrin"	411	2498904	1122632
9	ESZ	"Eschenz"	414	2707844	1278214
10	CEV	"Cevio"	417	2689688	1130564
[truncated]
```

## 5. Altitudes of stations 2

Here are the requested query and its output:

```sql
SELECT station,
	MAX(temperature) as temperature_max, altitude
FROM "meteoswiss_grh"."current"
INNER JOIN "stations" ON station = "stations".id
WHERE altitude >= 400 AND altitude < 500
GROUP BY station, altitude
ORDER BY altitude ASC
```

```
#	station	temperature_max	altitude
1	VEV	11.5	405
2	SCM		408
3	OBR	10.7	409
4	GVE	11.1	411
5	CEV	8.5	417
6	QUI		419
7	HLL	9.2	419
8	WYN	9.7	422
9	PRE		425
10	KLO	10.0	426
[truncated]
```
