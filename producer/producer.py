import boto3
import json
import time
import random
import argparse
import os
from datetime import datetime, timezone
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "subscription-payments-dev")
REGION = "us-west-2"
BATCH_SIZE = 500
MAX_WORKERS = 20

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Constants
TIERS = {
    "basic": 9.99,
    "standard": 15.49,
    "premium": 19.99,
    "basic_ads": 6.99
}

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "BR", "MX", "JP", "AU"]
PROCESSORS = ["stripe", "adyen"]


def generate_payment() -> Dict:
    """Generate subscription payment event with exact 85% success, 10% failure, 5% refund"""
    timestamp_ms = int(time.time() * 1000)
    random_suffix = random.randint(100000, 999999)
    
    customer_id = f"cus_{random.randint(100000, 9999999)}"
    user_id = f"user_{random.randint(10000000, 99999999)}"
    tier = random.choice(list(TIERS.keys()))
    country = random.choice(COUNTRIES)
    
    roll = random.random()
    if roll < 0.85:
        event_type = "payment.succeeded"
        status = "succeeded"
        amount = TIERS[tier]
        is_refund = False
        original_payment_id = None
    elif roll < 0.95:
        event_type = "payment.failed"
        status = "failed"
        amount = 0
        is_refund = False
        original_payment_id = None
    else:
        event_type = "payment.refunded"
        status = "succeeded"
        amount = -1 * TIERS[tier]
        is_refund = True
        original_payment_id = f"pay_{timestamp_ms}_{random.randint(100000, 999999)}"
    
    now = datetime.now(timezone.utc)
    
    return {
        "event_id": f"evt_{timestamp_ms}_{random_suffix}",
        "event_type": event_type,
        "event_timestamp": now.isoformat(),
        "payment_id": f"pay_{timestamp_ms}_{random_suffix}",
        "customer_id": customer_id,
        "user_id": user_id,
        "subscription_id": f"sub_{random.randint(10000000, 99999999)}",
        "amount_usd": float(amount),
        "currency": "USD",
        "status": status,
        "subscription_tier": tier,
        "country_code": country,
        "payment_processor": random.choice(PROCESSORS),
        "billing_period": now.strftime("%Y-%m"),
        "is_refund": is_refund,
        "original_payment_id": original_payment_id
    }


def send_batch_to_kinesis(events: List[Dict]) -> tuple:
    """Send batch of events to Kinesis stream"""
    try:
        records = [
            {
                "Data": json.dumps(event),
                "PartitionKey": event["customer_id"]
            }
            for event in events
        ]
        
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=records
        )
        
        failed_count = response.get("FailedRecordCount", 0)
        return len(events) - failed_count, failed_count
    except Exception as e:
        print(f"Error: {e}")
        return 0, len(events)


def run_producer(duration_seconds: int, events_per_second: int):
    """Run payment producer with parallel batch processing"""
    print(f"Stream: {STREAM_NAME}")
    print(f"Target Rate: {events_per_second:,} events/sec")
    print(f"Batch Size: {BATCH_SIZE}")
    print(f"Duration: {duration_seconds}s ({duration_seconds/60:.1f} min)\n")
    
    start_time = time.time()
    end_time = start_time + duration_seconds
    
    stats = {"sent": 0, "failed": 0}
    last_report_time = 0
    
    batches_per_second = max(1, events_per_second // BATCH_SIZE)
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while time.time() < end_time:
                batch_start = time.time()
                
                # Generate batches
                batches = [[generate_payment() for _ in range(BATCH_SIZE)] 
                          for _ in range(batches_per_second)]
                
                futures = [executor.submit(send_batch_to_kinesis, batch) for batch in batches]
                for future in as_completed(futures):
                    sent, failed = future.result()
                    stats["sent"] += sent
                    stats["failed"] += failed
                
                # Progress report every 10 seconds
                elapsed = int(time.time() - start_time)
                if elapsed >= last_report_time + 10 and stats["sent"] > 0:
                    current_rate = stats["sent"] / elapsed
                    print(f"[{elapsed}s] Sent: {stats['sent']:,} | Failed: {stats['failed']} | Rate: {current_rate:,.0f}/sec")
                    last_report_time = elapsed
                
                # Sleep to maintain rate
                elapsed_batch = time.time() - batch_start
                if elapsed_batch < 1.0:
                    time.sleep(1.0 - elapsed_batch)
                
    except KeyboardInterrupt:
        print("\nStopping producer...")
    
    # Final stats
    elapsed = time.time() - start_time
    actual_rate = stats["sent"] / elapsed if elapsed > 0 else 0
    total_cost = (stats["sent"] / 1_000_000) * 0.015
    
    print(f"\n{'='*60}")
    print("Producer Summary")
    print(f"{'='*60}")
    print(f"Duration: {elapsed:.1f}s")
    print(f"Events Sent: {stats['sent']:,}")
    print(f"Events Failed: {stats['failed']:,}")
    print(f"Actual Rate: {actual_rate:,.0f}/sec")
    print(f"Estimated Cost: ${total_cost:.4f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kinesis payment event producer")
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=1000,
        help="Events per second (default: 1000 for 1-shard setup)"
    )
    
    args = parser.parse_args()
    
    print(f"Starting producer...")
    print(f"Estimated cost: ${(args.duration * args.rate / 1_000_000) * 0.015:.4f}\n")
    
    run_producer(args.duration, args.rate)
