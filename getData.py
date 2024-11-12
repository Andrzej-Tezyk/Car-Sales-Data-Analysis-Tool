from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dotenv import load_dotenv

import os

# for username and pass
load_dotenv()

# db conf
database_url = "jdbc:postgresql://localhost:5432/cars"
database_properties = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# create spark session
spark = SparkSession.builder.master("local").appName("CarSalesAnalysis").getOrCreate()

# manually created schema, because of "saledate" column having format Spark can not automatically read
schema = (StructType()
        .add("year", IntegerType())
        .add("make", StringType())
        .add("model", StringType())
        .add("trim", StringType())
        .add("body", StringType())
        .add("transmission", StringType())
        .add("vin", StringType())
        .add("state", StringType())
        .add("condition", IntegerType())
        .add("odometer", IntegerType())
        .add("color", StringType())
        .add("interior", StringType())
        .add("seller", StringType())
        .add("mmr", IntegerType())
        .add("sellingprice", IntegerType())
        .add("saledate", StringType()))

# df creation
cars = spark.read.format("csv").option("header", True).schema(schema).load("data/car_prices.csv").createOrReplaceTempView("cars")

# data cleaning
cleaned_cars = spark.sql(""" SELECT
                    CASE
                        WHEN LOWER(make) IN ('vw') THEN 'volkswagen'
                        WHEN LOWER(make) IN ('chev truck') THEN 'chevrolet'
                        WHEN LOWER(make) IN ('dodge tk') THEN 'dodge'
                        WHEN LOWER(make) IN ('ford tk', 'ford truck') THEN 'ford'
                        WHEN LOWER(make) IN ('gmc truck') THEN 'gmc'
                        WHEN LOWER(make) IN ('hyundai tk') THEN 'hyundai'
                        WHEN LOWER(make) IN ('landrover') THEN 'land rover'
                        WHEN LOWER(make) IN ('mazda tk') THEN 'mazda'
                        WHEN LOWER(make) IN ('mercedes-b', 'mercedes-benz') THEN 'mercedes'
                        ELSE LOWER(make) 
                        END AS make,
          
                        CASE
                            WHEN LOWER(model) IN ('1') THEN '1 series'
                            WHEN LOWER(model) IN ('7') THEN '7 series'
                            WHEN LOWER(model) IN ('expeditn', 'expedit') THEN 'expedition'
                            WHEN LOWER(model) IN ('f250') THEN 'f-250'
                            WHEN LOWER(model) IN ('fusion energi') THEN 'fusion'
                            WHEN LOWER(model) IN ('ridgelin') THEN 'ridgeline'
                            WHEN LOWER(model) IN ('xj') THEN 'xj-series'
                            WHEN LOWER(model) IN ('xk') THEN 'xk-series'
                            WHEN LOWER(model) IN ('gr', 'grand') AND make IN ('jeep') THEN 'grand cherokee'
                            WHEN LOWER(model) IN ('range', 'rangerover') THEN 'range rover'
                            WHEN LOWER(model) IN ('mazdaspeed3') THEN 'mazdaspeed 3'
                            WHEN LOWER(model) IN ('protege5') THEN 'protege'
                            WHEN LOWER(model) IN ('rx8') THEN 'rx-8'
                            WHEN LOWER(model) IN ('mountnr') THEN 'mountaineer'
                            WHEN LOWER(model) IN ('cv tradesman') THEN 'c/v tradesman'
                            WHEN LOWER(model) IN ('xl7') THEN 'xl-7'
                            WHEN LOWER(model) IN ('gti') THEN 'golf gti'
                            ELSE LOWER(model)
                            END AS model,
          
                        LOWER(trim) AS trim,
                        LOWER(body) AS body,
                        LOWER(transmission) AS transmission,
                        
                        CASE 
                            WHEN condition < 12 THEN 'very bad'
                            WHEN condition BETWEEN 12 AND 24 THEN 'bad'
                            WHEN condition BETWEEN 25 AND 36 THEN 'average'
                            WHEN condition > 36 THEN 'good'
                        END AS condition,
          
                        (odometer * 1.609344) AS odometer_km,
          
                        CASE 
                            WHEN LENGTH(state) > 2 THEN 'unknown'
                            ELSE state
                        END AS state,
          
                        CASE
                            WHEN interior IN ('—') THEN 'unknown'
                            WHEN interior IS NULL THEN 'unknown'
                            ELSE LOWER(interior)
                        END AS interior,
          
                        CASE
                            WHEN color RLIKE '^[0-9]+$' THEN 'unknown'
                            WHEN color IN ('—') THEN 'unknown'
                            WHEN color IS NULL THEN 'unknown'
                            ELSE LOWER(color)
                        END AS color,

                        CASE
                            WHEN color IN ('—') THEN 'unknown'
                            WHEN seller IS NULL THEN 'unknown'
                            ELSE LOWER(seller)
                        END AS seller,
          
                        mmr,
                        sellingprice,
                        to_date(RIGHT(LEFT(saledate, 15), 11), 'MMM dd yyyy') AS saledate

                    FROM cars
                    WHERE
                        make != 'dot' AND make IS NOT NULL
           """)

# loading data to postgre db
cleaned_cars.write.jdbc(url="jdbc:postgresql://localhost:5432/cars", table="cars", mode="overwrite", properties=database_properties)

# show first 10 rows
cleaned_cars.show(10)
