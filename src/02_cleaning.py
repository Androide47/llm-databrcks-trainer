import dlt
from pyspark.sql.functions import col, length, lit, trim


@dlt.table(
    name="cleaned_llm_data",
    comment="Silver layer: cleaned text. ML toxicity scoring skipped (serverless memory); simple length/quality filters only.",
    table_properties={
        "quality": "silver",
    },
)
def cleaned_llm_data():
    """Read bronze UC table; trim and filter by length. No Detoxify / PyTorch."""
    df_ = spark.read.table("llm_curation.`01_bronze`.raw_llm_data")
    return (
        df_.filter(col("value").isNotNull())
        .withColumn("text", trim(col("value")))
        # Drop short lines (noise)
        .filter(length(col("text")) > lit(50))
        # Cap very long rows to keep chunking / serverless UDFs stable
        .filter(length(col("text")) < lit(200_000))
        # Simple spam heuristic: drop rows with 25+ repeated identical characters
        .filter(~col("text").rlike(r"(.)\1{24}"))
    )
