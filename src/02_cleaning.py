import dlt

from transforms.silver import apply_silver_transforms


@dlt.table(
    name="cleaned_llm_data",
    comment=(
        "Silver layer: cleaned text. ML toxicity scoring skipped (serverless memory); "
        "simple length/quality filters only."
    ),
    table_properties={
        "quality": "silver",
    },
)
def cleaned_llm_data():
    """Read bronze UC table; trim and filter by length. No Detoxify / PyTorch."""
    df_ = spark.read.table("llm_curation.`01_bronze`.raw_llm_data")
    return apply_silver_transforms(df_)
