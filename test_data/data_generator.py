import boto3
import requests

s3 = boto3.client('s3')
bucket_name = "amzn-s2-androide-bucket"

# A few "real-world" samples: some clean, some messy
samples = [
    "<html><body><h1>The History of Spark</h1><p>Apache Spark was started at UC Berkeley's AMPLab in 2009 by Matei Zaharia...</p></body></html>",
    "Contact the maintainer at matei@databricks.com for more info on Delta Lake. This is a great project!",
    "Toxic Comment: You are a complete idiot if you don't use Unity Catalog in 2026. This is garbage data.", # Should be caught by toxicity filter
    "Data Engineering is the process of designing and building systems for collecting, storing, and analyzing data at scale."
]

for i, text in enumerate(samples):
    s3.put_object(
        Bucket=bucket_name,
        Key=f"raw/sample_{i}.txt",
        Body=text
    )

print(f"Successfully seeded {len(samples)} files to S3.")