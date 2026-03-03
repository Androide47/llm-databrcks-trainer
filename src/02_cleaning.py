import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="cleaned_llm_data",
    comment="Silver layer: Cleaned deduplicated, and PII-masked text data",
    table_properties={
        "quality": "silver",
    }
)
#Cleaning data from raw_llm_data
def cleaned_llm_data():
    df_= dlt.read("raw_llm_data")
    df = df_.filter(col("value").isNotNull()) \
        .withColumn("text", trim(col("value"))) \
        .filtering(length(col("text")) > 50)
    html_regex = "<[^>]*>"
    df = df.withColumn("text", regexp_replace(col("text"), html_regex, ""))
    
    email_regex = fr"[\w\.-]+@[\w\.-]+\.\w+"
    df = df.withColumn("text", regexp_replace(col("text"), email_regex, "[EMAIL_MASKED]"))
    
    return df.select(
        "text",
        "ingestion_time",
        "source_file",
        sha2(col("text"), 256).alias("text_hash")
    )