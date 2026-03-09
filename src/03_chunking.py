import dlt
from pyspark.sql.functions import *
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

# Define the splitter logic

def chunk_text(text):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=200,
        length_function=len,
        )
    return splitter.split_text(text)

@pandas_udf("array<string>")
def chunk_text_udf(text: pd.Series) -> pd.Series:
    return text.apply(chunk_text)

@dlt.table(
    name="gold_llm_chunks",
    comment="Gold layer: Text chunked and ready for Vector Search/Embedding",
    table_properties={"quality": "gold"}
)

def gold_llm_chunks():
    return(
        dlt.read("cleaned_llm_data")
        .withColumn("chunks", chunk_text_udf(col("text")))
        .withColumn("chunk", explode(col("chunks")))
        .select(
            "soure_file",
            "content_hash",
            "chunk",
            sha2(col("chunk"), 256).alias("chunk_id")
        )
    )