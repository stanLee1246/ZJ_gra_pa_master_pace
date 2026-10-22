from pathlib import Path

import pytest

from ecommerce_research.config import load_config
from ecommerce_research.storage import spark_session


@pytest.fixture
def cfg():
    return load_config(Path(__file__).resolve().parents[1] / "configs" / "demo.yaml")


@pytest.fixture(scope="session")
def spark():
    cfg = load_config(Path(__file__).resolve().parents[1] / "configs" / "demo.yaml")
    session = spark_session(cfg)
    yield session
    session.stop()
