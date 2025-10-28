from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType
)

# ============================================================================
# SOURCE SCHEMAS
# ============================================================================

# netflix_raw.subscription_payments
SUBSCRIPTION_PAYMENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),  # Will convert to timestamp
    StructField("payment_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("subscription_id", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("subscription_tier", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("payment_processor", StringType(), True),
    StructField("billing_period", StringType(), True),
    StructField("is_refund", BooleanType(), True),
    StructField("original_payment_id", StringType(), True)
])

# netflix_raw.partner_invoices
PARTNER_INVOICE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("invoice_id", StringType(), True),
    StructField("partner_name", StringType(), True),
    StructField("billing_period", StringType(), True),
    StructField("total_subscriptions", IntegerType(), True),
    StructField("revenue_per_sub", DoubleType(), True),
    StructField("total_amount_usd", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("paid_date", StringType(), True),
    StructField("country_code", StringType(), True)
])

# netflix_raw.ad_revenue
AD_REVENUE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("advertiser_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("impression_count", IntegerType(), True),
    StructField("billable_impressions", IntegerType(), True),
    StructField("revenue_usd", DoubleType(), True),
    StructField("billing_period", StringType(), True)
])

# ============================================================================
# SCHEMA REGISTRY - Map schema names to schemas
# ============================================================================

SCHEMA_REGISTRY = {
    "subscription_payment": SUBSCRIPTION_PAYMENT_SCHEMA,
    "partner_invoice": PARTNER_INVOICE_SCHEMA,
    "ad_revenue": AD_REVENUE_SCHEMA
}

def get_schema(schema_name: str) -> StructType:
    """
    Retrieve schema by name
    
    Args:
        schema_name: Name of the schema (e.g., 'subscription_payment')
    
    Returns:
        PySpark StructType schema
    
    Raises:
        ValueError: If schema name not found
    """
    if schema_name not in SCHEMA_REGISTRY:
        available = ", ".join(SCHEMA_REGISTRY.keys())
        raise ValueError(
            f"Schema '{schema_name}' not found. "
            f"Available schemas: {available}"
        )
    
    return SCHEMA_REGISTRY[schema_name]