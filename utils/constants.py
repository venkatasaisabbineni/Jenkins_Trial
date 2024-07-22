import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

#Data
OUTPUT_PATH = parser.get('data','output_path')

# #AWS
# AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
# AWS_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
# AWS_REGION = parser.get('aws', 'aws_region')
# AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')

#API
DRIVERS = parser.get('api_methods', 'drivers')