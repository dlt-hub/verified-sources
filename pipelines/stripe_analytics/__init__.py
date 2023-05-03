"""
This pipeline uses Stripe API and dlt to load data such as Customer, Subscription, Event and etc. to the database and to calculate the MRR and churn rate.
"""
from .stripe_analytics import stripe_source, metrics_resource