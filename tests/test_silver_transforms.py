from pyspark.sql import Row

from transforms.silver import apply_silver_transforms

# Silver `rlike(r"(.)\1{24}")` drops rows with 25+ identical consecutive chars;
# sample strings must avoid that (e.g. no "x" * 60).


def _mixed_60() -> str:
    return ("abcdef" * 10)[:60]


def test_silver_drops_null_value(spark):
    ok = _mixed_60()
    df = spark.createDataFrame([Row(value=None), Row(value=ok)], schema="value string")
    out = apply_silver_transforms(df).collect()
    assert len(out) == 1
    assert out[0].text == ok


def test_silver_drops_short_text(spark):
    ok = _mixed_60()
    df = spark.createDataFrame([Row(value="short"), Row(value=ok)], schema="value string")
    out = apply_silver_transforms(df).collect()
    assert len(out) == 1
    assert out[0].text == ok


def test_silver_drops_very_long_text(spark):
    long_ok = _mixed_60()
    too_long = ("wx" * 100_001)[:200_001]
    df = spark.createDataFrame([Row(value=long_ok), Row(value=too_long)], schema="value string")
    out = apply_silver_transforms(df).collect()
    assert len(out) == 1
    assert out[0].text == long_ok


def test_silver_drops_repeated_char_spam(spark):
    spam = "a" * 25 + "b" * 60
    clean = _mixed_60()
    df = spark.createDataFrame([Row(value=spam), Row(value=clean)], schema="value string")
    out = apply_silver_transforms(df).collect()
    assert len(out) == 1
    assert out[0].text == clean


def test_silver_trims_whitespace(spark):
    inner = _mixed_60()
    # Spark `trim` removes leading/trailing spaces, not all Unicode whitespace (e.g. newline).
    df = spark.createDataFrame([Row(value=f"   {inner}   ")], schema="value string")
    out = apply_silver_transforms(df).collect()
    assert len(out) == 1
    assert out[0].text == inner
