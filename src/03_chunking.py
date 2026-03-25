import dlt
import pandas as pd
from pyspark.sql.functions import col, explode, pandas_udf, sha2

from transforms.chunking import chunk_text


@pandas_udf("array<string>")
def chunk_text_udf(text: pd.Series) -> pd.Series:
    return text.apply(chunk_text)


@dlt.table(
    name="gold_llm_chunks",
    comment="Gold layer: Text chunked and ready for Vector Search/Embedding",
    table_properties={"quality": "gold"},
)
def gold_llm_chunks():
    return (
        spark.read.table("llm_curation.`02_silver`.cleaned_llm_data")
        .withColumn("chunks", chunk_text_udf(col("text")))
        .withColumn("chunk", explode(col("chunks")))
        .select(
            "source_file",
            "chunk",
            sha2(col("chunk"), 256).alias("chunk_id"),
        )
    )
