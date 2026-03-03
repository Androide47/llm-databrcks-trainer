import dlt
from pyspark.sql.functions import *

source_path = "s3://amzn-s2-androide-bucket/raw/"

@dlt.table(
    name="raw_llm_data",
    comment="Raw text data ingested from S3 for LLM training",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.manage": "ture"
    }
)

def raw_llm_data():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation","s3://amzn-s2-androide-bucket/schemas/bronze")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .drop("_metadata")
    )