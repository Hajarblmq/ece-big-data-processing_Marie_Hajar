"""Helpers to read IMDb TSV.gz files into Spark DataFrames."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

from .config import RAW_DIR, IMDB_SCHEMAS, NULL_TOKEN


# Map de string → instance Spark DataType (Spark 4 refuse les strings)
_TYPE_MAP = {
    "string": StringType(),
    "int": IntegerType(),
    "double": DoubleType(),
}


def _build_schema(table: str) -> StructType:
    fields = [
        StructField(name, _TYPE_MAP[dtype], True)
        for name, dtype in IMDB_SCHEMAS[table]
    ]
    return StructType(fields)


def read_imdb(spark: SparkSession, table: str) -> DataFrame:
    """Read a TSV.gz IMDb table with proper null handling."""
    if table not in IMDB_SCHEMAS:
        raise ValueError(f"Unknown table {table!r}. Known: {list(IMDB_SCHEMAS)}")
    path = RAW_DIR / f"{table}.tsv.gz"
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    return (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", NULL_TOKEN)
        .option("emptyValue", None)
        .schema(_build_schema(table))
        .csv(str(path))
    )
