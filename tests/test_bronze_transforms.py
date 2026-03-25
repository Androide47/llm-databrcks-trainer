from pyspark.sql import Row
from pyspark.sql.functions import lit

from transforms.bronze import add_bronze_columns


def test_add_bronze_columns_adds_timestamp_and_source(spark):
    df = spark.createDataFrame([Row(value="hello")], schema="value string")
    out = add_bronze_columns(df, lit("s3://bucket/path/file.txt"))
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0].value == "hello"
    assert rows[0].source_file == "s3://bucket/path/file.txt"
    assert rows[0].ingestion_time is not None


def test_add_bronze_columns_drops_metadata_when_present(spark):
    df = spark.createDataFrame(
        [
            Row(value="x", _metadata='{"file_path":"p"}'),
        ],
        schema="value string, _metadata string",
    )
    out = add_bronze_columns(df, lit("p"))
    assert "_metadata" not in out.columns
