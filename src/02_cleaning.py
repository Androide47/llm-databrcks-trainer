import dlt
import pandas as pd
from detoxify import Detoxify
from pyspark.sql.functions import *

@pandas_udf("float")
def detect_toxicity(text: pd.Series) -> pd.Series:
    model = Detoxify("original")
    results = model.predict(batch.tolist())
    return pd.Series(results['toxicity'])

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
        .filter(length(col("text")) > 50)
    
    df = df.withColumn("toxicity", detect_toxicity(col("text")))
    
    return df.select(
        df.filter(col("toxicity") < 0.8)
    )