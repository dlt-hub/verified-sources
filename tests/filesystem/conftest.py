import pytest
from pathlib import Path


@pytest.fixture
def data_dir() -> str:
    current_dir = Path(__file__).parent.resolve()
    return (current_dir / "test_data").as_posix()
