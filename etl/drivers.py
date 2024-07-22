from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests
import s3fs
from utils.constants import DRIVERS,OUTPUT_PATH#,AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY,AWS_BUCKET_NAME

spark = SparkSession.builder \
    .appName("F1_Drivers to S3") \
    .getOrCreate()
print("Process Started")
#extract
response = requests.get(DRIVERS)
if response.status_code == 200:
    data = response.json()
else:
    print(f"Error {response.status_code}: {response.reason}")
    data = []
df = spark.createDataFrame(data)
df.printSchema()
#transformations
df = df.drop("headshot_url","team_colour")
df = df.withColumn("driver_number", col("driver_number").cast("int"))
df = df.withColumn("meeting_key", col("meeting_key").cast("int"))
df = df.withColumn("session_key", col("session_key").cast("int"))
df.dropDuplicates()
type_of_method = DRIVERS.split('/')[-1]
file_path = f'{OUTPUT_PATH}/{type_of_method}'
# df.write.option("header",True).partitionBy("meeting_key","session_key").csv(file_path)
#load
# try:
#     s3 = s3 = s3fs.S3FileSystem(anon=False,
#                                key= AWS_ACCESS_KEY_ID,
#                                secret=AWS_ACCESS_KEY)
# except Exception as e:
#     print(e)
# # #check if the bucket is there or not
# # try:
# #         if not s3.exists(AWS_BUCKET_NAME):
# #             s3.mkdir(AWS_BUCKET_NAME)
# #             print("Bucket created")
# #         else :
# #             print("Bucket already exists")
# # except Exception as e:
# #     print(e)
# #upload to the bucket
# try:
#         s3.put(file_path, AWS_BUCKET_NAME,recursive=True)
#         print('File uploaded to s3')
# except FileNotFoundError:
#     print('The file was not found')
spark.stop()