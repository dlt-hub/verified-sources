import pandas as pd
import pendulum
import pytest


@pytest.fixture()
def subscription_dataset():
    subscriptions = {
        "discount__coupon__duration": ["once", "forever", "forever"],
        "discount__coupon__percent_off": [100, 10, 50],
        "plan__interval": ["month", "month", "year"],
        "plan__amount": [1000, 10000, 120000],
        "quantity": [1, 1, 1],
        "created": [
            pendulum.today(),
            pendulum.today().subtract(days=30),
            pendulum.today().subtract(days=60),
        ],
        "status": ["canceled", "past_due", "active"],
    }
    df_sub = pd.DataFrame(subscriptions)

    return df_sub


@pytest.fixture()
def event_dataset():
    events = {
        "type": [
            "invoice.paid",
            "customer.subscription.deleted",
            "customer.discount.created",
            "payment_intent.succeeded",
            "customer.subscription.deleted",
        ],
    }
    df_event = pd.DataFrame(events)

    return df_event
