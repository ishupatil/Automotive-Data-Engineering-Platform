import os
import sys
from pathlib import Path
import csv
import shutil


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURATION
# ============================================================

os.environ["JAVA_HOME"] = r"D:\java"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

orders_path = Path(r"C:\Users\hp\Downloads\orders_initial_backup.csv")
customers_path = Path(r"C:\Users\hp\Downloads\customers.csv")
dealers_path = Path(r"C:\Users\hp\Downloads\dealers.csv")
vehicle_models_path = Path(r"C:\Users\hp\Downloads\vehicle_models.csv")

output_dir = Path(
    r"D:\github\Automotive-Data-Engineering-Platform\Databricks"
    r"\outputs\pyspark_pipeline_run"
)

ALLOWED_STATUSES = ["cancelled", "confirmed", "delivered", "pending"]
FINANCED_PAYMENT_MODES = ["emi", "lease", "loan"]
TEST_ORDER_PREFIX = "ORD-TEST-"
AMOUNT_TOLERANCE = 0.01


# ============================================================
# 2. HELPERS
# ============================================================

def require_columns(df, required_columns, dataframe_name):
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(
            f"{dataframe_name} is missing columns {missing}. "
            f"Available columns: {df.columns}"
        )


def count_rows(label, df):
    value = df.count()
    print(f"{label}: {value}")
    return value

def write_csv(df, folder_name):
    """Write a Spark DataFrame to a local CSV file using Python."""

    destination = output_dir / folder_name

    # Remove previous output so reruns don't mix old and new results.
    if destination.exists():
        shutil.rmtree(destination)

    destination.mkdir(parents=True, exist_ok=True)

    csv_path = destination / "data.csv"

    # Write the column headers and rows directly to a local CSV file.
    with csv_path.open(
        mode="w",
        newline="",
        encoding="utf-8-sig"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(df.columns)

        for row in df.toLocalIterator():
            writer.writerow([
                "" if value is None else str(value)
                for value in row
            ])

    print(f"Saved {folder_name}: {csv_path}")


def show_if_not_empty(label, df, limit=20):
    print(f"\n=== {label} ===")
    df.show(limit, truncate=False)


# ============================================================
# 3. VALIDATE PATHS AND CREATE OUTPUT DIRECTORY
# ============================================================

input_paths = {
    "orders": orders_path,
    "customers": customers_path,
    "dealers": dealers_path,
    "vehicle models": vehicle_models_path,
}

for label, path in input_paths.items():
    if not path.is_file():
        raise FileNotFoundError(
            f"{label.title()} CSV not found:\n{path}\n"
            "Edit the path in Section 1 of this script."
        )

# Creates outputs/pyspark_pipeline_run automatically if missing.
output_dir.mkdir(parents=True, exist_ok=True)


# ============================================================
# 4. START SPARK AND LOAD CSV FILES
# ============================================================

spark = (
    SparkSession.builder
    .appName("AutomotiveAutomatedLocalPipeline")
    .master("local[*]")
    .getOrCreate()
)

try:
    print("\n=== Loading CSV snapshots ===")

    orders_raw = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "FAILFAST")
        .csv(str(orders_path))
    )

    # Customers and dealers contain quoted multiline address fields.
    customers_raw = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .option("quote", '"')
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .csv(str(customers_path))
    )

    dealers_raw = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("mode", "FAILFAST")
        .csv(str(dealers_path))
    )

    vehicle_models_raw = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("mode", "FAILFAST")
        .csv(str(vehicle_models_path))
    )

    require_columns(
        orders_raw,
        [
            "order_id", "order_number", "customer_id", "dealer_id",
            "order_status", "payment_mode", "finance_provider",
            "total_amount", "discount_amount", "tax_amount", "net_amount",
        ],
        "orders",
    )
    require_columns(customers_raw, ["customer_id"], "customers")
    require_columns(dealers_raw, ["dealer_id"], "dealers")
    # Vehicle models are loaded and inspected. A join is not performed
    # because the confirmed orders schema has no known model_id join key.

    print("\n=== Source counts ===")
    source_orders_count = count_rows("Orders", orders_raw)
    customers_count = count_rows("Customers", customers_raw)
    dealers_count = count_rows("Dealers", dealers_raw)
    vehicle_models_count = count_rows("Vehicle models", vehicle_models_raw)

    print("\n=== Source schemas ===")
    print("Orders:")
    orders_raw.printSchema()
    print("Customers:")
    customers_raw.printSchema()
    print("Dealers:")
    dealers_raw.printSchema()
    print("Vehicle models:")
    vehicle_models_raw.printSchema()

    # Validate join-key types before joining.
    for df_name, df, key in [
        ("orders", orders_raw, "customer_id"),
        ("orders", orders_raw, "dealer_id"),
        ("customers", customers_raw, "customer_id"),
        ("dealers", dealers_raw, "dealer_id"),
    ]:
        key_type = dict(df.dtypes).get(key)
        if key_type not in ("int", "bigint", "smallint", "long", "short"):
            raise TypeError(
                f"{df_name}.{key} has type {key_type}; expected a numeric ID. "
                "Fix the CSV parsing/schema before joining."
            )

    # ========================================================
    # 5. NORMALIZE ORDERS AND EXCLUDE KNOWN TEST ARTIFACTS
    # ========================================================

    orders = (
        orders_raw
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("payment_mode", F.lower(F.trim(F.col("payment_mode"))))
        .withColumn("order_number", F.trim(F.col("order_number")))
        .withColumn("finance_provider", F.trim(F.col("finance_provider")))
    )

    is_test_order = F.coalesce(
        F.col("order_number").startswith(TEST_ORDER_PREFIX),
        F.lit(False),
    )

    test_records = orders.filter(is_test_order)
    processing_orders = orders.filter(~is_test_order)

    print("\n=== Test-record handling ===")
    count_rows("Original orders", orders)
    count_rows("Excluded test records", test_records)
    count_rows("Orders entering validation", processing_orders)

    # ========================================================
    # 6. VALIDATE CUSTOMER AND DEALER KEYS
    # ========================================================

    customer_key_issues = (
        customers_raw.groupBy("customer_id")
        .count()
        .filter(F.col("customer_id").isNull() | (F.col("count") > 1))
    )

    dealer_key_issues = (
        dealers_raw.groupBy("dealer_id")
        .count()
        .filter(F.col("dealer_id").isNull() | (F.col("count") > 1))
    )

    print("\n=== Reference-key checks ===")
    count_rows("Customer key issue groups", customer_key_issues)
    count_rows("Dealer key issue groups", dealer_key_issues)
    show_if_not_empty("Customer key issues", customer_key_issues)
    show_if_not_empty("Dealer key issues", dealer_key_issues)

    # Use distinct, non-null reference keys so duplicate reference rows
    # cannot multiply orders during the relationship checks.
    customer_keys = (
        customers_raw.select("customer_id")
        .filter(F.col("customer_id").isNotNull())
        .distinct()
        .withColumn("_customer_found", F.lit(True))
    )

    dealer_keys = (
        dealers_raw.select("dealer_id")
        .filter(F.col("dealer_id").isNotNull())
        .distinct()
        .withColumn("_dealer_found", F.lit(True))
    )

    # ========================================================
    # 7. DETECT DUPLICATE ORDER IDs
    # ========================================================

    duplicate_order_ids = (
        processing_orders
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id")
        .withColumn("_duplicate_order", F.lit(True))
    )

    print("\n=== Order duplicate checks ===")
    count_rows("Duplicate order ID groups", duplicate_order_ids)

    # ========================================================
    # 8. APPLY QUALITY RULES
    # ========================================================

    checked = (
        processing_orders
        .join(customer_keys, on="customer_id", how="left")
        .join(dealer_keys, on="dealer_id", how="left")
        .join(duplicate_order_ids, on="order_id", how="left")
        .withColumn(
            "missing_required_id",
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("dealer_id").isNull(),
        )
        .withColumn(
            "invalid_order_status",
            F.col("order_status").isNull()
            | (~F.col("order_status").isin(ALLOWED_STATUSES)),
        )
        .withColumn(
            "customer_not_found",
            F.col("customer_id").isNotNull()
            & F.col("_customer_found").isNull(),
        )
        .withColumn(
            "dealer_not_found",
            F.col("dealer_id").isNotNull()
            & F.col("_dealer_found").isNull(),
        )
        .withColumn(
            "duplicate_order_id",
            F.col("_duplicate_order").isNotNull(),
        )
        .withColumn(
            "nonpositive_total_amount",
            F.col("total_amount").isNull() | (F.col("total_amount") <= 0),
        )
        .withColumn(
            "negative_financial_amount",
            (F.col("total_amount") < 0)
            | (F.col("discount_amount") < 0)
            | (F.col("tax_amount") < 0)
            | (F.col("net_amount") < 0),
        )
        .withColumn(
            "missing_finance_provider",
            F.col("payment_mode").isin(FINANCED_PAYMENT_MODES)
            & (
                F.col("finance_provider").isNull()
                | (F.trim(F.col("finance_provider")) == "")
            ),
        )
        .withColumn(
            "calculated_net_amount",
            F.col("total_amount")
            - F.col("discount_amount")
            + F.col("tax_amount"),
        )
        .withColumn(
            "net_amount_mismatch",
            F.col("net_amount").isNotNull()
            & F.col("calculated_net_amount").isNotNull()
            & (
                F.abs(
                    F.col("net_amount") - F.col("calculated_net_amount")
                ) > F.lit(AMOUNT_TOLERANCE)
            ),
        )
    )

    # Hard failures go to quarantine. Financial anomalies and missing
    # finance providers are review flags, not automatic rejection rules.
    hard_failure = (
        F.col("missing_required_id")
        | F.col("invalid_order_status")
        | F.col("customer_not_found")
        | F.col("dealer_not_found")
        | F.col("duplicate_order_id")
        | F.col("nonpositive_total_amount")
    )

    checked = (
        checked
        .withColumn("hard_failure", hard_failure)
        .withColumn(
            "review_required",
            F.col("negative_financial_amount")
            | F.col("missing_finance_provider")
            | F.col("net_amount_mismatch"),
        )
        .withColumn(
            "quality_status",
            F.when(F.col("hard_failure"), "rejected")
            .when(F.col("review_required"), "review_required")
            .otherwise("valid"),
        )
    )

    rejection_reasons = [
        F.when(F.col("missing_required_id"), "missing_required_id"),
        F.when(F.col("invalid_order_status"), "invalid_order_status"),
        F.when(F.col("customer_not_found"), "customer_not_found"),
        F.when(F.col("dealer_not_found"), "dealer_not_found"),
        F.when(F.col("duplicate_order_id"), "duplicate_order_id"),
        F.when(F.col("nonpositive_total_amount"), "nonpositive_total_amount"),
    ]

    checked = checked.withColumn(
        "rejection_reason",
        F.concat_ws(";", *rejection_reasons),
    )

    review_reasons = [
        F.when(F.col("negative_financial_amount"), "negative_financial_amount"),
        F.when(F.col("missing_finance_provider"), "missing_finance_provider"),
        F.when(F.col("net_amount_mismatch"), "net_amount_mismatch"),
    ]

    checked = checked.withColumn(
        "review_reason",
        F.concat_ws(";", *review_reasons),
    )

    # ========================================================
    # 9. CREATE SILVER, QUARANTINE, AND REVIEW DATASETS
    # ========================================================

    internal_columns = [
        "_customer_found",
        "_dealer_found",
        "_duplicate_order",
        "hard_failure",
    ]

    silver_orders = (
        checked.filter(~F.col("hard_failure"))
        .drop(*internal_columns)
    )

    quarantine_orders = (
        checked.filter(F.col("hard_failure"))
        .drop(*internal_columns)
    )

    # Review-required records are a flagged subset of Silver.
    review_orders = (
        checked.filter(
            (~F.col("hard_failure")) & F.col("review_required")
        )
        .drop(*internal_columns)
    )

    test_records_output = test_records.withColumn(
        "quality_status", F.lit("excluded_test_record")
    )

    # ========================================================
    # 10. QUALITY REPORT AND RECONCILIATION
    # ========================================================

    metrics = [
        ("source_orders", source_orders_count),
        ("test_records_excluded", test_records.count()),
        ("orders_validated", processing_orders.count()),
        ("silver_orders", silver_orders.count()),
        ("quarantined_orders", quarantine_orders.count()),
        ("review_required_orders", review_orders.count()),
        (
            "missing_required_ids",
            checked.filter(F.col("missing_required_id")).count(),
        ),
        (
            "invalid_order_statuses",
            checked.filter(F.col("invalid_order_status")).count(),
        ),
        (
            "unmatched_customer_orders",
            checked.filter(F.col("customer_not_found")).count(),
        ),
        (
            "unmatched_dealer_orders",
            checked.filter(F.col("dealer_not_found")).count(),
        ),
        (
            "duplicate_order_id_records",
            checked.filter(F.col("duplicate_order_id")).count(),
        ),
        (
            "nonpositive_total_amounts",
            checked.filter(F.col("nonpositive_total_amount")).count(),
        ),
        (
            "negative_financial_amounts",
            checked.filter(F.col("negative_financial_amount")).count(),
        ),
        (
            "missing_finance_providers",
            checked.filter(F.col("missing_finance_provider")).count(),
        ),
        (
            "net_amount_mismatches",
            checked.filter(F.col("net_amount_mismatch")).count(),
        ),
        ("customer_key_issue_groups", customer_key_issues.count()),
        ("dealer_key_issue_groups", dealer_key_issues.count()),
        ("customer_snapshot_rows", customers_count),
        ("dealer_snapshot_rows", dealers_count),
        ("vehicle_model_snapshot_rows", vehicle_models_count),
    ]

    quality_report = spark.createDataFrame(
        metrics, ["metric", "record_count"]
    )

    print("\n=== AUTOMATED DATA QUALITY REPORT ===")
    quality_report.show(100, truncate=False)

    validated_count = processing_orders.count()
    silver_count = silver_orders.count()
    quarantine_count = quarantine_orders.count()

    if validated_count != silver_count + quarantine_count:
        raise RuntimeError(
            "Row-count reconciliation failed: "
            f"validated={validated_count}, silver={silver_count}, "
            f"quarantine={quarantine_count}"
        )

    print("\nReconciliation passed:")
    print(
        f"Validated ({validated_count}) = Silver ({silver_count}) "
        f"+ Quarantine ({quarantine_count})"
    )
    print(f"Review-required rows (subset of Silver): {review_orders.count()}")

    # ========================================================
    # 11. WRITE LOCAL CSV OUTPUTS
    # ========================================================

    print("\n=== WRITING OUTPUTS ===")
    write_csv(silver_orders, "silver_orders")
    write_csv(quarantine_orders, "quarantine_orders")
    write_csv(review_orders, "review_orders")
    write_csv(test_records_output, "excluded_test_records")
    write_csv(quality_report, "quality_report")

    print("\n=== PIPELINE COMPLETED SUCCESSFULLY ===")
    print(f"Output folder: {output_dir.resolve()}")

finally:
    spark.stop()
