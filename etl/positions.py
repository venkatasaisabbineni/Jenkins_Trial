from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import requests
from utils.constants import POSITIONS,OUTPUT_PATH

import os
import sys
os.environ['SPARK_HOME'] = "/Users/venkatasaisabbineni/Work/Spark"
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Positions to S3") \
    .getOrCreate()
print("Process Started")
#extract
response = requests.get(POSITIONS)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Error {response.status_code}: {response.reason}")
    data = []
df = spark.createDataFrame(data)
# df.printSchema()
#transformations
df = df.withColumn("date",to_timestamp("date"))
windowSpecLatest = Window.partitionBy("meeting_key", "session_key", "position").orderBy(col("date").desc())
df_latest = df.withColumn("row_number", row_number().over(windowSpecLatest)).filter(col("row_number") == 1).drop("row_number")
df_latest = df_latest.orderBy("position")
#load
type_of_method = POSITIONS.split('/')[-1]
type_of_method = type_of_method.split('?')[0]
file_path = f'{OUTPUT_PATH}/{type_of_method}'
df_latest.write.mode("overwrite").option("header",True).partitionBy("meeting_key","session_key").csv(file_path)
spark.stop()