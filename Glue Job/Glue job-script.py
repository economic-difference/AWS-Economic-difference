import boto3
import sys
import re
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, to_date, coalesce

# =====================================================
# JOB ARGUMENTS (EventBridge Lambda)
# =====================================================
args = getResolvedOptions(sys.argv, ["SOURCE_BUCKET", "SOURCE_KEY"])
SOURCE_BUCKET = args["SOURCE_BUCKET"]
SOURCE_KEY = args["SOURCE_KEY"]

print(f"Glue job triggered by: s3://{SOURCE_BUCKET}/{SOURCE_KEY}")

# =====================================================
# INITIALIZE
# =====================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client("s3")

# =====================================================
# CURATED PATHS
# =====================================================
CURATED_BUCKET = "economic-warehouse-curated"
MACRO_PATH  = f"s3://{CURATED_BUCKET}/warehouse/macro_master/"
MARKET_PATH = f"s3://{CURATED_BUCKET}/warehouse/market_master/"
DEV_PATH    = f"s3://{CURATED_BUCKET}/warehouse/development_master/"

# =====================================================
# HELPERS
# =====================================================
def archive_file(bucket, key):
    archive_key = f"archive/{key}"
    print(f"Archiving {key} → {archive_key}")
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=archive_key
    )
    s3.delete_object(Bucket=bucket, Key=key)


def read_existing(path):
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def extract_date_from_key(key):
    m = re.search(r"\d{4}-\d{2}-\d{2}", key)
    return m.group(0) if m else None


def find_matching_file(prefix, date_str):
    resp = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix)
    for obj in resp.get("Contents", []):
        if date_str in obj["Key"]:
            return obj["Key"]
    return None

# =====================================================
# ROUTING LOGIC
# =====================================================

# =====================================================
# 1 FRED UPSERT (Date)
# =====================================================
if SOURCE_KEY.startswith("fred/"):
    try:
        fred_df = (
            spark.read
            .option("header", "true")
            .csv(f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}")
        )

        fred_df = fred_df.withColumn(
            "Indicator",
            F.when(col("Indicator") == "CPIAUCSL", "CPI")
             .when(col("Indicator") == "VIXCLS", "VIX")
             .when(col("Indicator") == "DCOILBRENTEU", "BRENT")
             .otherwise(col("Indicator"))
        )

        fred_final = (
            fred_df
            .groupBy("Date")
            .pivot("Indicator")
            .agg(F.first("Value").cast("double"))
        )

        if fred_final.count() == 0:
            raise Exception("No valid FRED rows after pivot")

        # UPSERT (only addition)
        try:
            existing_df = spark.read.parquet(MACRO_PATH)
            combined_df = existing_df.unionByName(fred_final, allowMissingColumns=True)
        except Exception:
            combined_df = fred_final

        agg_exprs = [
            F.max(col(c)).alias(c)
            for c in combined_df.columns
            if c != "Date"
        ]

        final_upsert_df = combined_df.groupBy("Date").agg(*agg_exprs)

        final_upsert_df.write.mode("overwrite").partitionBy("Date").parquet(MACRO_PATH)

        archive_file(SOURCE_BUCKET, SOURCE_KEY)
        print("FRED UPSERT completed")

    except Exception as e:
        print(f"FRED ETL failed: {e}")
        raise

# =====================================================
#  WORLD BANK UPSERT (year + country)
# =====================================================
elif SOURCE_KEY.startswith("worldbank/"):
    try:
        new_df = spark.read.option("header", "true").csv(
            f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}"
        )

        new_df = new_df.select(
            col("year").cast("int"),
            col("country"),
            col("indicator"),
            col("value").cast("double")
        )

        existing = read_existing(DEV_PATH)
        combined = new_df if existing is None else existing.unionByName(new_df)

        final_df = (
            combined
            .groupBy("year", "country")
            .pivot("indicator")
            .agg(F.max("value"))
        )

        final_df.write.mode("overwrite").parquet(DEV_PATH)
        archive_file(SOURCE_BUCKET, SOURCE_KEY)

        print("World Bank UPSERT completed")

    except Exception as e:
        print(f"World Bank failed: {e}")
        raise

# =====================================================
# MARKET STRICT FAN-IN UPSERT (Date + country)
# =====================================================
elif SOURCE_KEY.startswith("yahoo/") or SOURCE_KEY.startswith("ecb/"):
    try:
        date_str = extract_date_from_key(SOURCE_KEY)
        if not date_str:
            print("No date in market filename — exit")
            sys.exit(0)

        yahoo_key = find_matching_file("yahoo/", date_str)
        ecb_key   = find_matching_file("ecb/", date_str)

        # HARD GUARD — do NOTHING unless BOTH exist
        if yahoo_key is None or ecb_key is None:
            print(
                f"Market incomplete for {date_str} "
                f"(Yahoo={bool(yahoo_key)}, ECB={bool(ecb_key)})"
            )
            sys.exit(0)

        yahoo_df = spark.read.option("header", "true").csv(
            f"s3://{SOURCE_BUCKET}/{yahoo_key}"
        )
        ecb_df = spark.read.option("header", "true").csv(
            f"s3://{SOURCE_BUCKET}/{ecb_key}"
        )

        new_df = (
            yahoo_df
            .join(ecb_df, ["Date", "country"], "inner")
            .groupBy("Date", "country")
            .agg(
                F.first("Close").alias("Index_Close"),
                F.first("EUR_Rate").alias("FX_EUR_Rate")
            )
        )

        existing = read_existing(MARKET_PATH)
        combined = new_df if existing is None else existing.unionByName(new_df)

        final_df = (
            combined
            .groupBy("Date", "country")
            .agg(
                F.max("Index_Close").alias("Index_Close"),
                F.max("FX_EUR_Rate").alias("FX_EUR_Rate")
            )
        )

        final_df.write.mode("overwrite").partitionBy("Date").parquet(MARKET_PATH)

        archive_file(SOURCE_BUCKET, yahoo_key)
        archive_file(SOURCE_BUCKET, ecb_key)

        print(f"Market UPSERT completed for {date_str}")

    except Exception as e:
        print(f"Market failed: {e}")
        raise

# =====================================================
# UNKNOWN SOURCE SAFE EXIT
# =====================================================
else:
    print("️ No matching ETL route")

print("ETL JOB FINISHED SUCCESSFULLY")