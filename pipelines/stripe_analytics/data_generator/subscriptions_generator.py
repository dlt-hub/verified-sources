import os
import random
from typing import Tuple

import stripe
from tqdm import tqdm

from pipelines.stripe_analytics.data_generator.random_data_generator import (
    RandomDataGenerator,
)


class StripeDataUploader:
    def __init__(self, api_key: str, api_version: str = "2022-11-15"):
        stripe.api_key = api_key
        stripe.api_version = api_version
        self.card_types = [
            "pm_card_visa",
            "pm_card_visa_debit",
            "pm_card_mastercard",
            "pm_card_mastercard_debit",
            "pm_card_mastercard_prepaid",
            "pm_card_unionpay",
        ]

    @staticmethod
    def create_subscription(names: Tuple[str, ...], amounts: Tuple[str, ...]):
        for name, amount in tqdm(zip(names, amounts), desc="Create subscriptions..."):
            starter_subscription = stripe.Product.create(
                name=f"Starter {name} Subscription",
                description=f"${round(float(amount)/100, 2)}/Month for {name} subscription",
            )

            stripe.Price.create(
                unit_amount=amount,
                currency="usd",
                recurring={"interval": "month"},
                product=starter_subscription["id"],
            )

    def create_customer(self, names: Tuple[str, ...]):
        for name in tqdm(names, desc="Create customers..."):
            splitted_name = name.split()
            card_picked = random.choice(self.card_types)

            stripe.Customer.create(
                name=name,
                email=f"{splitted_name[0]}_{splitted_name[1]}@example.com",
                payment_method=card_picked,
                invoice_settings={"default_payment_method": card_picked},
            )

    @staticmethod
    def attach_customer_and_subscription():
        all_customers = stripe.Customer.list()["data"]
        all_subscriptions = stripe.Price.list()["data"]

        for sample_customer in tqdm(
            all_customers, desc="Randomly assign subscriptions on customers..."
        ):
            stripe.Subscription.create(
                customer=sample_customer["id"],
                items=[{"price": random.choice(all_subscriptions)["id"]}],
                default_payment_method=sample_customer["invoice_settings"].get(
                    "default_payment_method", "pm_card_visa"
                ),
            )

    @staticmethod
    def randomly_delete_subscriptions(number: int = 5):
        all_customers_subscriptions = stripe.Subscription.list()["data"]
        to_delete = random.sample(all_customers_subscriptions, k=number)

        for sample in tqdm(to_delete, desc="Randomly delete some customer's subscriptions..."):
            stripe.Subscription.delete(sample["id"])


if __name__ == "__main__":
    secret_key = os.getenv("SECRET_KEY")
    uploader = StripeDataUploader(secret_key)
    generator = RandomDataGenerator()

    rand_plans = generator.random_plans_generation(2)
    rand_amounts = generator.random_amounts_generation(2)
    rand_customers = generator.random_names_generation(20)

    uploader.create_subscription(rand_plans, rand_amounts)
    uploader.create_customer(rand_customers)
    uploader.attach_customer_and_subscription()
    uploader.randomly_delete_subscriptions(5)
