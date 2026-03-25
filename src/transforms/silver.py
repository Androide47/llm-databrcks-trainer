from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lit, trim


def apply_silver_transforms(df: DataFrame) -> DataFrame:
    """Trim text, filter by length, drop simple repetition spam. Matches silver DLT table logic."""
    return (
        df.filter(col("value").isNotNull())
        .withColumn("text", trim(col("value")))
        .filter(length(col("text")) > lit(50))
        .filter(length(col("text")) < lit(200_000))
        .filter(~col("text").rlike(r"(.)\1{24}"))
    )
