from datetime import datetime

import numpy as np
import pandas as pd


def calculate_mrr(df_sub: pd.DataFrame) -> float:
    """
    Monthly Recurring Revenue (MRR) can be thought of as the total
    amount of monthly revenue you can reliably expect to receive on a recurring basis.
    You can calculate the approximate MRR by summing the monthly-normalized
    amounts of all subscriptions from which payment is being collected at that time.
    """
    # COUPON
    # If the customer has a coupon attached,
    # make sure to take that into account for revenue.
    # Only when coupon duration is forever that coupon affects MRR.
    df_sub["discount__coupon__percent_off"] = np.where(
        df_sub["discount__coupon__duration"] == "forever",
        df_sub["discount__coupon__percent_off"],
        0,
    )
    # NORMALIZED PLAN AMOUNT
    # Year subscriptions need to be divided by 12.
    # Monthly revenue divided by 100, because Stripe gives revenue in cents
    df_sub["plan_amount_month"] = np.where(
        df_sub["plan__interval"] == "month",
        (1 / 100)
        * df_sub["plan__amount"]
        * df_sub["quantity"]
        * (1 - df_sub["discount__coupon__percent_off"] / 100),
        np.where(
            df_sub["plan__interval"] == "year",
            (1 / 100)
            * (1 / 12)
            * df_sub["plan__amount"]
            * df_sub["quantity"]
            * (1 - df_sub["discount__coupon__percent_off"] / 100),
            np.nan,
        ),
    )

    df_sub["created"] = pd.to_datetime(df_sub["created"], unit="s").dt.tz_localize(None)

    def total_mrr(df_sub: pd.DataFrame, end_date: datetime = datetime(2023, 5, 1)) -> float:
        """
        Total MRR
        end_date: first day of the next month
        """
        df_sub = df_sub[df_sub["created"] < end_date]
        return df_sub[df_sub["status"].isin(["active", "past_due"])][
            "plan_amount_month"
        ].sum()

    return total_mrr(df_sub)


def churn_rate(df_event: pd.DataFrame, df_subscription: pd.DataFrame) -> float:
    """
    The churn rate is measured by the sum of churned subscribers
    in the past 30 days divided by the number of active subscribers
    as of 30 days ago, plus any new subscribers in those 30 days.
    """
    # churned subscribers in the past 30 days
    churned_subscriber = len(
        df_event[df_event["type"] == "customer.subscription.deleted"]
    )
    # total active or past_due subscription now
    subscriber = len(df_subscription[df_subscription["status"] != "canceled"])

    return round(churned_subscriber / (churned_subscriber + subscriber), 3)