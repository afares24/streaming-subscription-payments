# Real-Time Payment Analytics Pipeline

A configurable streaming data pipeline that processes subscription payments in real-time, built entirely with AWS Kinesis, Spark, Iceberg, and Terraform.

## What It Does

Ingests payment events at scale (500-10,000/sec), transforms them in real-time, and stores them in a queryable data lakehouse - all with sub-minute latency from event to insight.

## Tech Stack

- **AWS Kinesis** - Event streaming
- **AWS Glue** - Spark-based stream processing
- **Apache Iceberg** - ACID-compliant table format
- **AWS Athena** - SQL analytics
- **Terraform** - Infrastructure as Code
- **Python** - Event generation & processing

## Architecture
```
Payment Events → Kinesis Stream → Glue Streaming Job → Iceberg Table → Athena Queries
   (Python)       (1-15 shards)      (Spark ETL)         (S3 + Glue)    (SQL)
```

## Key Features

**Real-time processing** - Sub-minute end-to-end latency  
**Auto-scaling** - 1 to 15 Kinesis shards via config  
**ACID transactions** - Reliable writes with Iceberg  
**Idempotent processing** - Prevents duplicate records 
**Compression** - ZSTD for 30% storage savings and fast decompresion
**IaC deployment** - Full Terraform automation  

## Quick Start
```bash
# Deploy infrastructure
cd terraform
terraform init
terraform apply -var-file="environments/dev.tfvars"

# Start streaming job
aws glue start-job-run \
  --job-name netflix_revenue-kinesis-to-iceberg-dev \
  --region us-west-2

# Generate payment events (wait 2-3 min after starting job)
cd ../producer
python producer.py --duration 60 --rate 500

# Query results in AWS Athena
SELECT subscription_tier, COUNT(*), SUM(amount_usd)
FROM netflix_revenue_raw.subscription_payments
WHERE status = 'succeeded'
GROUP BY subscription_tier;
```

## System Design Decisions

### Apache Iceberg over Delta Lake
- Better AWS Glue integration
- ACID transactions for streaming
- Time travel capabilities
- Schema evolution without downtime

### ZSTD Compression
- 20-30% better compression than GZIP
- Faster query performance
- Lower storage costs

### Batch Deduplication
- Fast micro-batch processing
- Event-ID based idempotency
- Trade-off: Speed over absolute consistency

### Scalable Architecture
- **Dev mode:** 1 shard, ~$15/month
- **Demo mode:** 15 shards, ~$165/month

## Sample Event
```json
{
  "event_id": "evt_1730000000000_123456",
  "event_type": "payment.succeeded",
  "customer_id": "cus_1234567",
  "amount_usd": 15.49,
  "subscription_tier": "standard",
  "status": "succeeded",
  "country_code": "US",
  "payment_processor": "stripe"
}
```

**Event Distribution:** 85% successful, 10% failed, 5% refunds

## Project Structure
```
├── terraform/          # Infrastructure as Code
│   ├── kinesis.tf     # Stream configuration
│   ├── glue.tf        # Spark job setup
│   ├── iam.tf         # Permissions
│   └── environments/  # Dev/demo configs
├── glue/              # Spark streaming job
│   ├── job.py        # Main entry point
│   ├── framework.py  # Reusable streaming framework
│   └── schemas.py    # Event schemas
└── producer/          # Event generator
    └── producer.py   # Payment simulator
```

## Technical Highlights

**Infrastructure as Code**
- Complete deployment in one command
- Environment-specific configurations
- Easy scaling between dev and production

**Stream Processing**
- Micro-batch architecture (seconds latency)
- Automatic checkpointing and recovery
- Idempotent writes with deduplication

**Data Lake Architecture**
- Iceberg provides ACID guarantees
- Optimized parquet files with ZSTD
- Query via standard SQL (Athena)

**Production Ready**
- CloudWatch logging and monitoring
- IAM least-privilege permissions
- Configurable retention policies

## Stream Scale Testing

| Configuration | Shards | Events/sec | Cost/Month |
|--------------|--------|------------|------------|
| Development  | 1      | ~1,000     | ~$15       |
| Demo         | 15     | ~15,000    | ~$165      |


## Clean Up
```bash
cd terraform
terraform destroy -auto-approve
```

All resources removed in ~2 minutes.

---
