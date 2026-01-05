import pandas as pd, boto3, io, os
from datetime import datetime

# Extract
csv_files = [
    f"C:\\Users\\user\\Desktop\\MediTrack360\\services\\extract\\lab_results\\lab_results_2025-11-{i:02d}.csv"
    for i in range(1, 11)
]
combined_df = pd.concat(
    [
        pd.read_csv(f).assign(source_file=os.path.basename(f))
        for f in csv_files
        if os.path.exists(f)
    ]
)

# Load to S3
buffer = io.BytesIO()
combined_df.to_parquet(buffer, index=False)
boto3.client("s3", region_name="us-east-2").put_object(
    Bucket="meditrack360-data-lake-6065273c",
    Key=f"bronze/lab_results/{datetime.now().strftime('%Y/%m/%d')}/lab_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet",
    Body=buffer.getvalue(),
)

print(f"✅ Uploaded {len(combined_df)} rows to S3")
