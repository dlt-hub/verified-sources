import pytest


@pytest.fixture
def queries():
    return {
      "recipient_company_name": "Who is the recipient of the invoice? Just return the name. If you don't know, then return None",
      "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text. If you don't know, then return None",
    }


