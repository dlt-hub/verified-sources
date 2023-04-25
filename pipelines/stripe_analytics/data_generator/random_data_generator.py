import random
from typing import Any, Tuple

from faker import Faker


class RandomDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.fake.seed_instance(42)

    def random_names_generation(self, length: int = 10) -> Tuple[str]:
        random_names = []
        for _ in range(length):
            random_names.append(str(self.fake.name()))

        return tuple(random_names)

    def random_plans_generation(self, length: int = 2) -> Tuple[str]:
        random_plans = []
        for _ in range(length):
            random_plans.append(str(self.fake.word()))

        return tuple(random_plans)

    @staticmethod
    def random_amounts_generation(length: int = 2) -> Tuple[str]:
        random_amounts = []
        for _ in range(length):
            random_amounts.append(str(random.randint(100, 10000)))

        return tuple(random_amounts)

    def random_cards_generation(
        self, length: int = 10
    ) -> tuple[dict[str, int | Any], ...]:
        """
        This function is not used, because of Stripe restrictions.
        """
        random_cards = []
        brands = [
            "American Express",
            "Diners Club",
            "Discover",
            "Eftpos Australia",
            "JCB",
            "MasterCard",
            "UnionPay",
            "Visa",
            "Unknown",
        ]
        for _ in range(length):
            random_cards.append(
                {
                    "brand": random.choice(brands),
                    "number": str(self.fake.credit_card_number()),
                    "exp_month": random.randint(1, 12),
                    "exp_year": random.randint(2024, 2035),
                    "cvc": str(self.fake.credit_card_security_code()),
                }
            )

        return tuple(random_cards)
