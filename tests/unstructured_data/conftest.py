from pathlib import Path
from typing import Sequence

import pytest


@pytest.fixture
def queries():
    return {
        "recipient_company_name": "Who is the recipient of the invoice? Just return the name. If you don't know, then return None",
        "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text. If you don't know, then return None",
    }


@pytest.fixture
def data_dir() -> str:
    current_dir = Path(__file__).parent.resolve()
    return (current_dir / "test_data").as_posix()


@pytest.fixture(scope="session")
def gd_folders() -> Sequence[str]:
    return ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]
