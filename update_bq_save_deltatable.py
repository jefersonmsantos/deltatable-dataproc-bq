from delta import DeltaTable
from pyspark.sql import functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

conf.set("spark.jars.packages", "io.delta:delta-core:1.0.0")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
)

spark = SparkSession.builder.appName("bq_test").config(conf=conf).getOrCreate()

# read data from Big Query
taxi_data = (
    spark.read.format("bigquery")
    .option("table", "bigquery-public-data.new_york.tlc_yellow_trips_2015")
    .load()
)

# filter data
taxi_filtered = taxi_data.filter(
    "pickup_datetime >= '2015-01-01' and pickup_datetime <'2015-03-01'"
)

# create report by Taxi driver
taxi_driver_report = taxi_filtered.groupBy("vendor_id").agg(
    f.sum("total_amount").alias("Total")
)

# read last version of saved report
old_report = DeltaTable.forPath(spark, "gs://bq_deltatable/taxi_driver_report")

# compare reports and MERGE / UPDATE
(
    old_report.alias("old")
    .merge(
        taxi_driver_report.alias("new"),
        """old.vendor_id = new.vendor_id""",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# print data
taxi_driver_report.show(20)
