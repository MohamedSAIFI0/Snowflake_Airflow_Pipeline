import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

local_folder = r"/home/saifi/Desktop/eccomerce_project/data"

for filename in os.listdir(local_folder):
    local_path = os.path.join(local_folder, filename)
    s3_path = filename
    s3.upload_file(local_path, BUCKET_NAME, s3_path)
    print(f"{filename} uploaded to s3 bucket {BUCKET_NAME}")