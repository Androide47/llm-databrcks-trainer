import os
import sys

import pytest
from pyspark.sql import SparkSession

# Ensure local mode uses the same interpreter for driver and workers (avoids mixed 3.11/3.13).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("llm-databrcks-trainer-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
