import dlt
from pyspark.sql.functions import col

from transforms.bronze import add_bronze_columns

source_path = "s3://amzn-s2-androide-bucket/raw/"


@dlt.table(
    name="raw_llm_data",
    comment="Raw text data ingested from S3 for LLM training",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.manage": "true",
    },
)
def raw_llm_data():
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", "s3://amzn-s2-androide-bucket/schemas/bronze")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )
    return add_bronze_columns(stream_df, col("_metadata.file_path"))
