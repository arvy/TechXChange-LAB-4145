"""
tests/conftest.py

Shared fixtures and markers for the workshop test suite.

Markers:
  integration — tests that require live external services (Kafka, watsonx.data).
                Skip with: pytest -m "not integration"
"""

from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load credentials from .env files at session start so all tests see them.
# The project root is two levels up from this file (tests/conftest.py).
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent

def pytest_configure(config):
    # mcp_ingestion_server/.env is loaded last so its values win on any conflict
    load_dotenv(_ROOT / "producer" / ".env", override=False)
    load_dotenv(_ROOT / "mcp_ingestion_server" / ".env", override=True)
    config.addinivalue_line(
        "markers",
        "integration: requires live external services (Kafka, watsonx.data)",
    )
