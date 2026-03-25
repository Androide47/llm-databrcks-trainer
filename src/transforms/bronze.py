from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import current_timestamp


def add_bronze_columns(df: DataFrame, source_file_col: Column) -> DataFrame:
    """Add ingestion timestamp and source path; drop Auto Loader _metadata when present."""
    out = df.withColumn("ingestion_time", current_timestamp()).withColumn(
        "source_file", source_file_col
    )
    if "_metadata" in out.columns:
        out = out.drop("_metadata")
    return out
