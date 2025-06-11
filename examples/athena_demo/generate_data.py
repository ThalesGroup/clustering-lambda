import gzip
import json
import random

RECORDS_NUMBER = 20_000

CITIES = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "Austin, TX",
    "Jacksonville, FL",
    "Fort Worth, TX",
    "Columbus, OH",
    "Charlotte, NC",
    "San Francisco, CA",
    "Indianapolis, IN",
    "Seattle, WA",
    "Denver, CO",
    "Washington, DC",
    "Nashville, TN",
    "Oklahoma City, OK",
    "El Paso, TX",
    "Boston, MA",
    "Portland, OR",
    "Las Vegas, NV",
    "Detroit, MI",
    "Memphis, TN",
    "Louisville",
]

genders = ["Male", "Female"]


def random_record(customer_id):
    return {
        "customer_id": customer_id,
        "gender": random.choice(genders),
        "age": random.randint(18, 70),
        "city": random.choice(CITIES),
        "product_ids": random.sample(range(1, 1000), random.randint(1, 5)),
    }


file_path = "example_data.jsonl.gz"

print("START: Generating records...")

with gzip.open(file_path, "wt", encoding="utf-8") as f:
    for i in range(1, RECORDS_NUMBER + 1):
        record = random_record(i)
        f.write(json.dumps(record) + "\n")

print(f"FINISHED: Generated {RECORDS_NUMBER} records in {file_path}")
