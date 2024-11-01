from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
#from pyspark.sql.functions import col, isnan, when, count

spark = SparkSession.builder.master("local").appName("CarSalesAnalysis").getOrCreate()

schema = StructType() \
        .add("year", IntegerType()) \
        .add("make", StringType()) \
        .add("model", StringType()) \
        .add("trim", StringType()) \
        .add("body", StringType()) \
        .add("transmission", StringType()) \
        .add("vin", StringType()) \
        .add("state", StringType()) \
        .add("condition", IntegerType()) \
        .add("odometer", IntegerType()) \
        .add("color", StringType()) \
        .add("interior", StringType()) \
        .add("seller", StringType()) \
        .add("mmr", IntegerType()) \
        .add("sellingprice", IntegerType()) \
        .add("saledate", DateType())

spark.read.format("csv").option("header", True).schema(schema).load("data/car_prices.csv").createOrReplaceTempView("cars")

spark.sql(""" 

select distinct saledate from cars



 """).show(1000)