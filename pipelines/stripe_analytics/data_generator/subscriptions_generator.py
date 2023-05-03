import os
import random
from typing import Tuple

import stripe
from random_data_generator import RandomDataGenerator
from tqdm import tqdm


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
        self.customers = []
        self.plans = []
        self.coupons = [None]
        self.subscriptions = []

    def create_subscription(self, names: Tuple[str, ...]):
        for name in tqdm(names, desc="Create subscriptions..."):
            amount = str(random.randint(100, 10000))

            starter_subscription = stripe.Product.create(
                name=f"Starter {name} Subscription",
                description=f"${round(float(amount)/100, 2)}/Month for {name} subscription",
            )

            plan = stripe.Price.create(
                unit_amount=amount,
                currency="usd",
                recurring={"interval": random.choice(["month", "year"])},
                product=starter_subscription["id"],
            )

            self.plans.append(plan.id)

    def create_customer(self, names: Tuple[str, ...]):
        for name in tqdm(names, desc="Create customers..."):
            splitted_name = name.split()
            card_picked = random.choice(self.card_types)

            customer = stripe.Customer.create(
                name=name,
                email=f"{splitted_name[0]}_{splitted_name[1]}@example.com",
                payment_method=card_picked,
                invoice_settings={"default_payment_method": card_picked},
            )

            self.customers.append(customer)

    def create_coupons(self):
        coupon1 = stripe.Coupon.create(
            duration="once",
            id=f"free-period_{random.randint(0, 1000000)}",
            percent_off=100,
        )
        coupon2 = stripe.Coupon.create(
            duration="forever",
            id=f"discount_{random.randint(0, 1000000)}",
            percent_off=15,
        )

        self.coupons.extend([coupon1.id, coupon2.id])

    def attach_customer_and_subscription(self):
        for sample_customer in tqdm(
            self.customers, desc="Randomly assign subscriptions on customers..."
        ):
            sub = stripe.Subscription.create(
                customer=sample_customer.id,
                items=[{"price": random.choice(self.plans)}],
                default_payment_method=sample_customer.invoice_settings.default_payment_method,
                coupon=random.choice(self.coupons),
            )

            self.subscriptions.append(sub)

    def randomly_delete_subscriptions(self, number: int = 5):
        to_delete = random.sample(self.subscriptions, k=number)

        for sample in tqdm(
            to_delete, desc="Randomly delete some customer's subscriptions..."
        ):
            stripe.Subscription.delete(sample.id)


if __name__ == "__main__":
    secret_key = os.getenv("SECRET_KEY")
    uploader = StripeDataUploader(secret_key)
    generator = RandomDataGenerator()

    rand_plans = generator.random_plans_generation(2)
    rand_customers = generator.random_names_generation(50)

    uploader.create_subscription(rand_plans)
    uploader.create_customer(rand_customers)
    uploader.create_coupons()
    uploader.attach_customer_and_subscription()
    uploader.randomly_delete_subscriptions(10)
