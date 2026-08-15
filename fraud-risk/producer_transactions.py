"""
producer_transactions.py

Simulates a stream of card transactions and publishes them to a Kafka topic.
Use this to generate demo traffic for the fraud-detection pipeline.

Run:
    pip install kafka-python faker
    python producer_transactions.py --bootstrap-servers localhost:9092 --topic transactions --rate 5
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "fuel", "dining",
    "online_retail", "utilities", "entertainment", "jewelry", "atm_withdrawal",
]

# A handful of "risky" categories/geographies we bias fraud into, so the demo
# dataset has a learnable signal instead of pure noise.
HIGH_RISK_CATEGORIES = {"jewelry", "electronics", "atm_withdrawal"}


def generate_account_pool(n=200):
    return [str(uuid.uuid4()) for _ in range(n)]


def generate_transaction(account_pool, fraud_rate=0.02):
    account_id = random.choice(account_pool)
    category = random.choice(MERCHANT_CATEGORIES)
    is_fraud = random.random() < fraud_rate

    if is_fraud:
        # Fraudulent transactions skew: larger amount, high-risk category,
        # a foreign-looking location relative to the account's "home".
        amount = round(random.uniform(500, 5000), 2)
        category = random.choice(list(HIGH_RISK_CATEGORIES))
        location = fake.country_code()
    else:
        amount = round(random.uniform(5, 400), 2)
        location = fake.country_code()

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "amount": amount,
        "merchant_category": category,
        "location": location,
        "event_time": datetime.now(timezone.utc).isoformat(),
        # label is only present in the simulator so we can train a supervised
        # model; in production this would come from a labeled fraud-ops feed,
        # not from the transaction event itself.
        "_simulated_label": int(is_fraud),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--accounts", type=int, default=200)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    account_pool = generate_account_pool(args.accounts)
    interval = 1.0 / args.rate

    print(f"Publishing to topic '{args.topic}' at ~{args.rate} events/sec. Ctrl+C to stop.")
    try:
        while True:
            txn = generate_transaction(account_pool)
            producer.send(args.topic, key=txn["account_id"], value=txn)
            print(txn)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
