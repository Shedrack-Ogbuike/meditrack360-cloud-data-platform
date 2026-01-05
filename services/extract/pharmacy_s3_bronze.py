import pandas as pd
import requests
import boto3
import io
from datetime import datetime

# 1. Extract
url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"
data = requests.get(url).json()
df = pd.DataFrame(data["drugs"])

# 2. Upload to S3
s3 = boto3.client("s3", region_name="us-east-2")
bucket = "meditrack360-data-lake-6065273c"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
key = f"bronze/pharmacy/pharmacy_{timestamp}.csv"

# Convert and upload
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

print(f"✅ Uploaded {len(df)} records to s3://{bucket}/{key}")
