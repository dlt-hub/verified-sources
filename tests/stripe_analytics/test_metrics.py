from sources.stripe_analytics.metrics import calculate_mrr, churn_rate


class TestMetrics:
    def test_mrr(self, subscription_dataset):
        mrr = calculate_mrr(subscription_dataset)
        assert mrr == 140, mrr

    def test_churn(self, subscription_dataset, event_dataset):
        churn = churn_rate(event_dataset, subscription_dataset)
        assert churn == 0.5, churn
