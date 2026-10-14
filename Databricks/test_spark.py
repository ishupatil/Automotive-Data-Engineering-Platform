
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Use the Java and Python installations already configured
os.environ["JAVA_HOME"] = r"D:\java"
os.environ["PYSPARK_PYTHON"] = sys.executable

orders_path = Path(
    r"C:\Users\hp\Downloads\orders_initial_backup.csv"
)
customers_path = Path(
    r"C:\Users\hp\Downloads\customers.csv"
)
dealers_path = Path(
    r"C:\Users\hp\Downloads\dealers.csv"
)
vehicle_models_path = Path(
    r"C:\Users\hp\Downloads\vehicle_models.csv"
)

if not orders_path.is_file():
    raise FileNotFoundError(
        f"Orders snapshot not found: {orders_path}"
    )

if not customers_path.is_file():
    raise FileNotFoundError(
        f"Customers snapshot not found: {customers_path}"
    )
if not dealers_path.is_file():
    raise FileNotFoundError(
        f"Dealers snapshot not found: {dealers_path}"
    )
if not vehicle_models_path.is_file():
    raise FileNotFoundError(
        f"Vehicle models snapshot not found: {vehicle_models_path}"
    )

spark = (
    SparkSession.builder
    .appName("AutomotiveBronzeInspection")
    .master("local[*]")
    .getOrCreate()
)

try:
    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(orders_path))
    )
    print("=== Orders schema ===")
    orders.printSchema()

    print("=== First 5 orders ===")
    orders.show(5, truncate=False)

    print("=== Total snapshot rows ===")
    print(orders.count())

    customers = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")
    .csv(str(customers_path))
)
    print("=== Customers schema ===")
    customers.printSchema()

    print("=== First 5 customers ===")
    customers.show(5, truncate=False)

    print("=== Total snapshot rows ===")
    print(customers.count())

    dealers = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("mode", "FAILFAST")
    .load(str(dealers_path))
)

    print("=== Dealers schema ===")
    dealers.printSchema()

    print("=== First 5 dealers ===")
    dealers.show(5, truncate=False)

    print("=== Total snapshot rows ===")
    print(dealers.count())

     

    vehicle_models = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(vehicle_models_path))
    )
    print("=== Vehicle Models schema ===")
    vehicle_models.printSchema()

    print("=== First 5 vehicle models ===")
    vehicle_models.show(5, truncate=False)

    print("=== Total snapshot rows ===")
    print(vehicle_models.count())

       
    
    # Count rows with missing order_id
    null_order_ids=orders.filter(
        col ("order_id").isNull()
        ).count()
    print("Rows with null order_id:", null_order_ids)


    #Check duplicates 
    duplicate_ids=(orders.groupby("order_id").agg(count("order_id").alias("record_count")).filter(col("record_count")>1))
    print("Duplicate order IDs:")
    duplicate_ids.show(truncate=False)

    print("Number of duplicated order IDs:", duplicate_ids.count())
    #Checking negative values of total_amount
    negative_total_amount = (
    orders
    .filter(col("total_amount") <= 0)
    .select("order_id", "order_number", "total_amount")
    .orderBy(col("total_amount").asc())
)
    print("Orders with negative or zero total_amount:", negative_total_amount.count())
    negative_total_amount.show(truncate=False)


    null_cust_ids = (
    orders
    .filter(col("customer_id").isNull())
    .select("order_id", "customer_id")
    )

    print("Orders with null customer_id:", null_cust_ids.count())
    null_cust_ids.show(truncate=False)

    orders.select("customer_id").printSchema()
    customers.select("customer_id").printSchema()

    orders.select("customer_id").show(5)
    customers.select("customer_id").show(5)

    print("Orders columns:")
    print(orders.columns)

    print("Customers columns:")
    print(customers.columns)

    print("Customer ID schema:")
    customers.select("customer_id").printSchema()

    print("Customer IDs and addresses:")
    customers.select(
    "customer_id",
    "address_line1"
).show(10, truncate=False)
    orphan_customer_ids = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            col("o.customer_id") == col("c.customer_id"),
            "left_anti"
        )
        .select(
            col("o.order_id"),
            col("o.customer_id")
        )
    )

    # 5. Display results
    print("Orphan customer records:")
    orphan_customer_ids.show(truncate=False)

    print(
        "Number of orphan orders:",
        orphan_customer_ids.count()
    )
    orders.printSchema()
    orders = orders.withColumn(
    "order_status",
    trim(col("order_status"))
)

    orders.select(
       "order_id",
       "order_status"
).show(10, truncate=False)

    #Distinct order statuses
    distinct_order_statuses = orders.select("order_status").distinct()
    distinct_order_statuses.show(truncate=False)


    #Lowercase order statuses
    orders = orders.withColumn(
    "order_status",
    lower(col("order_status"))


)

    missing_finance_provider=orders.filter(col("finance_provider").isNull()).count()
    missing_finance_provider=orders.filter(col("finance_provider").isNull()).select("order_id", "finance_provider")

    missing_finance_provider.show()

    print("Rows with null finance_provider:", missing_finance_provider.count())

    # Summarize missing finance_provider by payment_mode
    missing_finance_summary = (
    orders
    .filter(col("finance_provider").isNull())
    .groupBy("payment_mode")
    .count()
    .orderBy(col("count").desc())
)

    missing_finance_summary.show(truncate=False)

    # Identify financed orders with missing finance_provider
    missing_financed_orders = (
    orders
    .filter(
        col("finance_provider").isNull()
        & col("payment_mode").isin("EMI", "Lease", "Loan")
    )
    .select(
        "order_id",
        "payment_mode",
        "finance_provider",
        "total_amount"
    )
)

    missing_financed_orders.show(20, truncate=False)

    print("Missing provider for financed orders:", missing_financed_orders.count())

    # Summarize finance_provider by payment_mode
    finance_provider_summary = (
    orders
    .groupBy("payment_mode", "finance_provider")
    .count()
    .orderBy("payment_mode", col("count").desc())
)

    finance_provider_summary.show(truncate=False)

     # Check schema of orders DataFrame
    orders.select(
    "order_id",
    "order_date",
    "total_amount",
    "discount_amount",
    "tax_amount",
    "net_amount",
    "created_at"
).show(10, truncate=False)
    orders.printSchema()

    # Check if net_amount is equal to total_amount - discount_amount + tax_amount

    amount_check = orders.select(
    "order_id",
    "total_amount",
    "discount_amount",
    "tax_amount",
    "net_amount",
    (
        col("total_amount")- col("discount_amount")+ col("tax_amount")
    ).alias("calculated_net_amount")
)

    amount_check.show(10, truncate=False)

    # Check for mismatches between net_amount and calculated_net_amount

    mismatched_amounts = amount_check.filter(
    abs(
        col("net_amount") - col("calculated_net_amount")
    ) > 0.01
)

    print("Potential amount mismatches:", mismatched_amounts.count())
    mismatched_amounts.show(10, truncate=False)

    invalid_amounts = orders.filter(
    (col("total_amount") < 0)
    | (col("discount_amount") < 0)
    | (col("tax_amount") < 0)
    | (col("net_amount") < 0)
)

    print("Orders with negative amounts:", invalid_amounts.count())

    invalid_amounts.select(
    "order_id",
    "total_amount",
    "discount_amount",
    "tax_amount",
    "net_amount"
).show(10, truncate=False)

    print("Order status values:")
    orders.groupBy("order_status").count().orderBy("order_status").show(truncate=False)

    print("Payment mode values:")
    orders.groupBy("payment_mode").count().orderBy("payment_mode").show(truncate=False)

    #Standardize order_status and payment_mode columns by trimming whitespace and converting to lowercase
    orders = (
    orders
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("payment_mode", lower(trim(col("payment_mode"))))

    
)   #trim 
    print("Order status values:")
    orders.groupBy("order_status").count().show()

    print("Payment mode values:")
    orders.groupBy("payment_mode").count().show()



  # Find orders whose payment mode is "test"
    # We investigate these records before deciding whether to remove them.
    test_orders = orders.filter(
    col("payment_mode") == "test"
)

# Display useful columns to understand these records
    test_orders.select(
    "order_id",
    "order_number",
    "order_status",
    "payment_mode",
    "total_amount",
    "finance_provider"
).show(truncate=False)

# Count the records requiring investigation
    print("Test payment records:", test_orders.count())
# Find records that look like test orders
    test_records = orders.filter(
    col("order_number").startswith("ORD-TEST-")
)

# Inspect their identifiers and amounts
    test_records.select(
    "order_id",
    "order_number",
    "payment_mode",
    "total_amount"
).show(truncate=False)


# Identify test records for audit and investigation
    test_records = orders.filter(
    col("order_number").startswith("ORD-TEST-")
)

# Exclude confirmed test artifacts from the Silver candidate dataset
    silver_orders = orders.filter(
    ~col("order_number").startswith("ORD-TEST-")
)

# Validate the separation
    print("Original rows:", orders.count())
    print("Test records:", test_records.count())
    print("Silver candidate rows:", silver_orders.count())

# Inspect the excluded records
    test_records.select(
    "order_id",
    "order_number",
    "payment_mode"
).show(truncate=False)
    
# Group orders by ID and count the occurrences of each ID
    duplicate_orders = (
    silver_orders
    .groupBy("order_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

# Display IDs that occur more than once
    duplicate_orders.show(truncate=False)

# Count the duplicated IDs
    print("Duplicated order IDs:", duplicate_orders.count())

    # Flag records missing an order, customer, or dealer ID
    missing_required_fields = silver_orders.filter(
    col("order_id").isNull()
    | col("customer_id").isNull()
    | col("dealer_id").isNull()
)

# Display the affected records
    missing_required_fields.select(
    "order_id",
    "customer_id",
    "dealer_id"
).show(truncate=False)

# Count the affected records
    print(
    "Orders missing required IDs:",
    missing_required_fields.count()
    
)


    # Define the order statuses allowed by our current project rules
    valid_statuses = ["cancelled", "confirmed", "delivered", "pending"]

# Find orders with missing or unexpected statuses
    invalid_statuses = orders.filter(
    col("order_status").isNull() | (~col("order_status").isin(valid_statuses))
)

# Display records that need investigation
    invalid_statuses.select(
    "order_id",
    "order_status"
).show(truncate=False)

# Count invalid status records
    print("Invalid status records:", invalid_statuses.count())

    # Find orders where at least one financial amount is negative
# Negative values may be legitimate refunds or adjustments, so investigate first
    negative_amounts = orders.filter(
    (col("total_amount") < 0)
    | (col("discount_amount") < 0)
    | (col("tax_amount") < 0)
    | (col("net_amount") < 0)
)

# Display the affected orders for inspection
    negative_amounts.select(
    "order_id",
    "total_amount",
    "discount_amount",
    "tax_amount",
    "net_amount"
).show(10, truncate=False)

# Count orders that contain at least one negative amount
    print("Orders with negative amounts:", negative_amounts.count())

    # Find financed orders with no finance provider recorded
# Cash payments are excluded from this check
    missing_finance_provider = orders.filter(
    col("payment_mode").isin("emi", "lease", "loan")
    & col("finance_provider").isNull()
)

# Display the records for investigation
    missing_finance_provider.select(
    "order_id",
    "payment_mode",
    "finance_provider",
    "total_amount"
).show(20, truncate=False)

# Count affected orders
    print(
    "Financed orders missing provider:",
    missing_finance_provider.count()
)
    # Transformation 5 
    # Find customer IDs that are null or occur more than once.
# Duplicate customer IDs can multiply rows during a join.
    customer_key_issues = (
    customers
    .groupBy("customer_id")
    .count()
    .filter(
        col("customer_id").isNull()
        | (col("count") > 1)
    )
)

    customer_key_issues.show(truncate=False)

    print("Customer key issue groups:", customer_key_issues.count())

   
# Check whether dealer_id contains unexpected text
    dealers.select("dealer_id").show(20, truncate=False)

# Inspect the first 10 complete records
    dealers.show(10, truncate=False)


    
# 1. Check total dealer records
    print("Dealer count:", dealers.count())

# 2. Check missing dealer IDs
    print(
    "Missing dealer IDs:",
    dealers.filter(col("dealer_id").isNull()).count()
)

# 3. Check duplicate dealer IDs
    dealer_duplicates = (
    dealers.groupBy("dealer_id")
    .count()
    .filter(
        col("dealer_id").isNotNull()
        & (col("count") > 1)
    )
)

    print("Duplicate dealer ID groups:", dealer_duplicates.count())

# Inspect sample records
    dealers.select(
    "dealer_id", "dealer_name", "address_line1"
).show(10, truncate=False)


    # Find orders whose dealer_id does not exist in the dealers DataFrame
    unmatched_dealers = silver_orders.join(
    dealers.select("dealer_id"),
    on="dealer_id",
    how="left_anti"
)

    print("Orders without matching dealers:", unmatched_dealers.count())

    unmatched_dealers.select(
    "order_id", "dealer_id"
).show(20, truncate=False)





finally:
    spark.stop()
