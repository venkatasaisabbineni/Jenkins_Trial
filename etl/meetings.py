from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests
from utils.constants import MEETINGS,OUTPUT_PATH

import os
import sys
os.environ['SPARK_HOME'] = "/Users/venkatasaisabbineni/Work/Spark"
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Meetings to S3") \
    .getOrCreate()
print("Process Started")
#extract
response = requests.get(MEETINGS)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Error {response.status_code}: {response.reason}")
    data = []
df = spark.createDataFrame(data)
df.printSchema()
#transformations
type_of_method = MEETINGS.split('/')[-1]
file_path = f'{OUTPUT_PATH}/{type_of_method}'
df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("year").csv(file_path)
spark.stop()