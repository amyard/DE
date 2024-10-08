"""
This script generates mock data resembling Stripe charge API payloads as
well as customer satisfaction scores.
"""

import csv
import random
import time
from faker import Faker
import numpy as np
import os
import logging

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fake = Faker()

AMOUNT_OF_FILE_TO_GENERATE = 3

def generate_mock_data(folder_path: str):
    folder_validation(folder_path)
    produce_and_save_mock_data(folder_path)


def folder_validation(folder_path: str):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logging.info(f"folder was created - '{folder_path}'.")


def generate_charge(stripe_id, country_code, customer_id, satisfaction_speed, product_type):
    base_amount = random.randint(100, 200)

    multiplier = 1
    if satisfaction_speed >= 6 and product_type == "product_B":
        multiplier = random.uniform(10, 20)
    elif satisfaction_speed >= 6:
        multiplier = random.uniform(5, 10)

    amount = int(base_amount * multiplier)
    successful = random.random() < 0.95

    return {
        "id": stripe_id,
        "amount": amount if successful else random.randint(1, amount),
        "amount_captured": amount if successful else 0,
        "amount_refunded": 0 if successful else random.randint(0, 50),
        "currency": "usd",
        "created": int(time.time()),
        "customer_id": customer_id,
        "description": fake.sentence(),
        "disputed": not successful,
        "outcome_network_status": "approved_by_network" if successful else "declined_by_network",
        "outcome_reason": None,
        "outcome_risk_level": random.choice(["normal", "elevated", "highest"]),
        "outcome_risk_score": random.randint(0, 100),
        "paid": successful,
        "payment_intent": f"pi_{fake.uuid4()}",
        "payment_method": f"pm_{fake.uuid4()}",
        "payment_method_details_card_brand": random.choice(["visa", "mastercard", "amex"]),
        "payment_method_details_card_country": country_code,
        "status": "succeeded" if successful else "failed",
    }

def generate_satisfaction(customer_id, country_code):
    """Generate a satisfaction score that creates a cluster using
    product B rating speed very highly."""

    satisfaction_speed = random.randint(0, 10)
    satisfaction_product = random.randint(0, 10)
    satisfaction_service = random.randint(0, 10)

    if satisfaction_speed >= 6:
        product_type = np.random.choice(["product_A", "product_B"], p=[0.1, 0.9])
    else:
        product_type = np.random.choice(["product_A", "product_B"], p=[0.9, 0.1])

    return {
        "customer_id": customer_id,
        "customer_satisfaction_speed": satisfaction_speed,
        "customer_satisfaction_product": satisfaction_product,
        "customer_satisfaction_service": satisfaction_service,
        "product_type": product_type,
    }


def produce_and_save_mock_data(folder_path: str):
    for i in range(1, AMOUNT_OF_FILE_TO_GENERATE):
        logging.info(f"produce_and_save_mock_data item_{i}")
        charge_filename = f"{folder_path}/stripe_charge_data_{i}.csv"
        satisfaction_filename = f"{folder_path}/stripe_satisfaction_data_{i}.csv"

        with open(charge_filename, mode="w", newline="") as charge_file, open(
            satisfaction_filename, mode="w", newline="") as satisfaction_file:

            charge_writer = csv.writer(charge_file)
            satisfaction_writer = csv.writer(satisfaction_file)

            charge_writer.writerow(
                [
                    "id",
                    "amount",
                    "amount_captured",
                    "amount_refunded",
                    "currency",
                    "created",
                    "customer_id",
                    "description",
                    "disputed",
                    "outcome_network_status",
                    "outcome_reason",
                    "outcome_risk_level",
                    "outcome_risk_score",
                    "paid",
                    "payment_intent",
                    "payment_method",
                    "payment_method_details_card_brand",
                    "payment_method_details_card_country",
                    "status",
                ]
            )

            satisfaction_writer.writerow(
                [
                    "customer_id",
                    "customer_satisfaction_speed",
                    "customer_satisfaction_product",
                    "customer_satisfaction_service",
                    "product_type",
                ]
            )

            for _ in range(random.randint(100, 301)):
                customer_id = fake.uuid4()
                country_code = "US" if random.random() < 0.6 else fake.country_code()

                charge_count = 1
                if random.random() < 0.2:
                    charge_count = 2
                elif random.random() < 0.3:
                    charge_count = random.randint(3, 6)

                satisfaction = generate_satisfaction(customer_id, country_code)

                for _ in range(charge_count):
                    stripe_id = fake.uuid4()
                    charge = generate_charge(
                        stripe_id,
                        country_code,
                        customer_id,
                        satisfaction["customer_satisfaction_speed"],
                        satisfaction["product_type"],
                    )
                    charge_writer.writerow(
                        [
                            charge["id"],
                            charge["amount"],
                            charge["amount_captured"],
                            charge["amount_refunded"],
                            charge["currency"],
                            charge["created"],
                            charge["customer_id"],
                            charge["description"],
                            charge["disputed"],
                            charge["outcome_network_status"],
                            charge["outcome_reason"],
                            charge["outcome_risk_level"],
                            charge["outcome_risk_score"],
                            charge["paid"],
                            charge["payment_intent"],
                            charge["payment_method"],
                            charge["payment_method_details_card_brand"],
                            charge["payment_method_details_card_country"],
                            charge["status"],
                        ]
                    )

                satisfaction_writer.writerow(
                    [
                        satisfaction["customer_id"],
                        satisfaction["customer_satisfaction_speed"],
                        satisfaction["customer_satisfaction_product"],
                        satisfaction["customer_satisfaction_service"],
                        satisfaction["product_type"],
                    ]
                )