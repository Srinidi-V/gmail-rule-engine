"""
Pytest fixtures for all tests
"""
import pytest
import tempfile
import json
from datetime import datetime, timedelta
from src.database import EmailDatabase
from src.rule_engine import RuleEngine
from src.rule_validator import RuleValidator

# Test database configuration (override via env if needed)
import os

TEST_DB_CONFIG = {
    "dbname": os.getenv("DB_TEST_NAME", "gmail_rules_test"),
    "user": os.getenv(
        "DB_TEST_USER", os.getenv("USER")
    ),  # default to current macOS user
    "password": os.getenv("DB_TEST_PASSWORD", ""),
    "host": os.getenv("DB_TEST_HOST", "localhost"),
    "port": int(os.getenv("DB_TEST_PORT", 5432)),
}


@pytest.fixture(scope="function")
def test_db():
    """Provide clean test database for each test"""
    db = EmailDatabase(TEST_DB_CONFIG)

    # Clear all data before test
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM emails")
    conn.commit()
    cursor.close()
    conn.close()

    yield db

    # Cleanup after test
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM emails")
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture
def sample_email():
    """Standard sample email for testing"""
    return {
        "id": "test_email_001",
        "thread_id": "thread_001",
        "from": "sender@example.com",
        "to": "receiver@example.com",
        "subject": "Test Subject",
        "message": "Test message content",
        "received_date": datetime.now(),
        "labels": ["INBOX", "UNREAD"],
    }


@pytest.fixture
def sample_emails_batch():
    """Batch of sample emails"""
    base_time = datetime.now()
    return [
        {
            "id": f"email_{i:03d}",
            "thread_id": f"thread_{i:03d}",
            "from": f"sender{i}@example.com",
            "to": "receiver@example.com",
            "subject": f"Subject {i}",
            "message": f"Message content {i}",
            "received_date": base_time - timedelta(days=i),
            "labels": ["INBOX", "UNREAD"] if i % 2 == 0 else ["INBOX"],
        }
        for i in range(10)
    ]


@pytest.fixture
def temp_rules_file():
    """Create temporary rules file"""

    def _create_rules(rules_data):
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(rules_data, temp_file)
        temp_file.close()
        return temp_file.name

    return _create_rules


@pytest.fixture
def valid_rule():
    """Valid rule for testing"""
    return {
        "name": "Test Rule",
        "predicate": "all",
        "conditions": [
            {"field": "from", "predicate": "contains", "value": "example.com"}
        ],
        "actions": [{"type": "mark_as_read"}],
    }
