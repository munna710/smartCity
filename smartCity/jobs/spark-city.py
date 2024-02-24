from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql import DataFrame

def main():
    spark = SparkSession.builder.appName('SmartCity')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1","org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.469") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#vehicleschema
vehicle_schema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("fuelType", StringType(), True),
    

])
gps_schema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicle_type", StringType(), True),
])

traffic_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("camera_id", StringType(), True),
    StructField("snapshot", StringType(), True),
    StructField("location", StringType(), True),
])

weather_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", StringType(), True),
]) 

emergency_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("incident_id", StringType(), True),
])

def read_kafka_topic(topic, schema):
    return (spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", topic)\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(from_json(col("value").cast("string"), schema).alias("data"))\
    .select("data.*")
    .withWatermark('timestamp', '10 seconds')
    )

def streamWriter(input:DataFrame,checkpointFolder,output):
    return (input.writeStream\
    .format("parquet")\
    .option("path", output)\
    .option("checkpointLocation", checkpointFolder)\
    .outputMode("append")\
    .start())
  

query1 = streamWriter(vehicleDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/vehicle', output='s3a://spark-streaming-data/data/vehicle')
query2 = streamWriter(gpsDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/gps', output='s3a://spark-streaming-data/data/gps')
query3 = streamWriter(trafficDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/traffic', output='s3a://spark-streaming-data/data/traffic')
query4 = streamWriter(weatherDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/weather', output='s3a://spark-streaming-data/data/weather')
query5 = streamWriter(emergencyDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/emergency', output='s3a://spark-streaming-data/data/emergency')

query5.awaitTermination()

if __name__ == "__main__":
    main()
