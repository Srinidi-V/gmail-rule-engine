"""
Tests for database based overall operations
"""
from datetime import datetime
import time


class TestDatabaseBasics:
    """
    Tests for database basics
    """

    def test_create_tables(self, test_db):
        """Test table creation"""
        conn = test_db.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'emails'
            )
        """
        )
        assert cursor.fetchone()[0] == True

        cursor.execute(
            """
            SELECT COUNT(*) FROM pg_indexes 
            WHERE tablename = 'emails'
        """
        )
        index_count = cursor.fetchone()[0]
        assert index_count >= 4  # Should have 4+ indexes

        cursor.close()
        conn.close()

    def test_insert_single_email(self, test_db, sample_email):
        """Test inserting a single email"""
        test_db.insert_or_update_email(sample_email)

        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert retrieved[0]["id"] == sample_email["id"]
        assert retrieved[0]["from"] == sample_email["from"]

    def test_insert_batch_emails(self, test_db, sample_emails_batch):
        """Test batch email insertion"""
        test_db.insert_emails_batch(sample_emails_batch)

        emails = test_db.get_all_emails()
        assert len(emails) == len(sample_emails_batch)

    def test_get_email_by_id(self, test_db, sample_email):
        """Test retrieving email by ID"""
        test_db.insert_or_update_email(sample_email)

        retrieved = test_db.get_email_by_id(sample_email["id"])
        assert retrieved is not None
        assert retrieved["id"] == sample_email["id"]

    def test_get_email_by_id_not_found(self, test_db):
        """Test retrieving non-existent email"""
        retrieved = test_db.get_email_by_id("nonexistent_id")
        assert retrieved is None

    def test_count_emails(self, test_db, sample_emails_batch):
        """Test email counting"""
        assert test_db.count_emails() == 0

        test_db.insert_emails_batch(sample_emails_batch)
        assert test_db.count_emails() == len(sample_emails_batch)

    def test_get_stats(self, test_db, sample_emails_batch):
        """Test database statistics"""
        test_db.insert_emails_batch(sample_emails_batch)

        stats = test_db.get_stats()
        assert stats["unique_emails"] == len(sample_emails_batch)
        assert stats["current_versions"] == len(sample_emails_batch)
        assert stats["historical_versions"] == 0

    def test_insert_email_with_empty_fields(self, test_db):
        """Test inserting email with missing/empty fields"""
        email = {
            "id": "test_empty",
            "from": "",
            "to": "",
            "subject": None,
            "message": "",
            "received_date": None,
            "labels": [],
        }

        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert retrieved[0]["from"] == ""
        assert retrieved[0]["subject"] is None

    def test_insert_email_with_very_long_subject(self, test_db):
        """Test email with extremely long subject"""
        email = {
            "id": "test_long",
            "from": "sender@example.com",
            "subject": "A" * 10000,
            "message": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }

        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert len(retrieved[0]["subject"]) == 10000

    def test_insert_email_with_special_characters(self, test_db):
        """Test email with special characters"""
        email = {
            "id": "test_special",
            "from": "sender+tag@example.com",
            "subject": "Test \"quoted\" subject with 'quotes' and <html>",
            "message": "Message with\nnewlines\tand\ttabs",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }

        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert '"quoted"' in retrieved[0]["subject"]

    def test_insert_email_with_unicode_and_emojis(self, test_db):
        """Test email with unicode and emoji characters"""
        email = {
            "id": "test_unicode",
            "from": "sender@例え.jp",
            "subject": "Hello 世界 🌍 Test 🎉",
            "message": "Content with 中文 and العربية",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }

        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert "世界" in retrieved[0]["subject"]
        assert "🌍" in retrieved[0]["subject"]

    def test_insert_duplicate_email_id(self, test_db, sample_email):
        """Test inserting same email ID twice"""
        test_db.insert_or_update_email(sample_email)
        test_db.insert_or_update_email(sample_email)

        emails = test_db.get_all_emails()
        assert len(emails) == 1


class TestBitemporalTracking:
    """
    Tests for bitemporal tracking of email state changes
    """

    def test_first_insert_creates_current_version(self, test_db, sample_email):
        """Test that first insert creates current version"""
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 1
        assert history[0]["is_current"] == True
        assert history[0]["valid_to"] is None

    def test_unchanged_email_no_new_version(self, test_db, sample_email):
        """Test that unchanged email doesn't create new version"""
        test_db.insert_or_update_email(sample_email)

        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 1  # Still only 1 version

    def test_changed_labels_create_new_version(self, test_db, sample_email):
        """Test that label changes create new version"""
        # Insert initial version
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)  # Small delay to ensure different timestamps

        # Change labels to INBOX
        sample_email["labels"] = ["INBOX"]  # UNREAD removed
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 2

        # First version should be invalidated
        assert history[0]["is_current"] == False
        assert history[0]["valid_to"] is not None

        # Second version should be current
        assert history[1]["is_current"] == True
        assert history[1]["valid_to"] is None

    def test_multiple_label_changes(self, test_db, sample_email):
        """Test multiple label changes create multiple versions"""
        # Initial version: INBOX, UNREAD
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        # Second version: INBOX (UNREAD removed)
        sample_email["labels"] = ["INBOX"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        # Third version: INBOX, IMPORTANT
        sample_email["labels"] = ["INBOX", "IMPORTANT"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        # Fourth version: IMPORTANT (INBOX removed)
        sample_email["labels"] = ["IMPORTANT"]
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 4

        # Only the last version should be current
        current_versions = [v for v in history if v["is_current"]]
        assert len(current_versions) == 1
        assert current_versions[0]["labels"] == ["IMPORTANT"]

    def test_get_all_emails_returns_only_current(self, test_db, sample_email):
        """Test that get_all_emails returns only current versions"""
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["INBOX"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["IMPORTANT"]
        test_db.insert_or_update_email(sample_email)

        emails = test_db.get_all_emails()
        assert len(emails) == 1
        assert emails[0]["labels"] == ["IMPORTANT"]

    def test_count_emails_counts_unique_emails(self, test_db, sample_emails_batch):
        """Test that count_emails counts unique emails, not versions"""
        test_db.insert_emails_batch(sample_emails_batch)

        email = sample_emails_batch[0]
        for i in range(5):
            time.sleep(0.01)
            email["labels"] = [f"LABEL_{i}"]
            test_db.insert_or_update_email(email)

        count = test_db.count_emails()
        assert count == len(sample_emails_batch)

    def test_stats_show_historical_versions(self, test_db, sample_email):
        """Test that stats correctly show historical versions"""
        for i in range(5):
            time.sleep(0.01)
            sample_email["labels"] = [f"VERSION_{i}"]
            test_db.insert_or_update_email(sample_email)

        stats = test_db.get_stats()
        assert stats["unique_emails"] == 1
        assert stats["total_versions"] == 5
        assert stats["current_versions"] == 1
        assert stats["historical_versions"] == 4

    def test_rapid_successive_updates(self, test_db, sample_email):
        """Test many rapid updates in short time"""
        for i in range(10):
            sample_email["labels"] = [f"LABEL_{i}"]
            test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 10

        timestamps = [h["valid_from"] for h in history]
        assert len(set(timestamps)) == 10  # All unique

    def test_five_emails_same_sender_same_time(self, test_db):
        """Test 5 different emails from same sender at exact same time"""
        same_time = datetime.now()
        same_sender = "boss@company.com"

        emails = [
            {
                "id": f"email_{i}",
                "from": same_sender,
                "subject": f"Project {i}",
                "message": f"Content {i}",
                "received_date": same_time,
                "labels": ["INBOX", "UNREAD"],
            }
            for i in range(5)
        ]

        test_db.insert_emails_batch(emails)

        all_emails = test_db.get_all_emails()
        assert len(all_emails) == 5

        assert all(e["from"] == same_sender for e in all_emails)

        ids = [e["id"] for e in all_emails]
        assert len(set(ids)) == 5

    def test_subject_change_not_tracked(self, test_db, sample_email):
        """Test that subject changes are not tracked"""
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        # Change subject
        sample_email["subject"] = "Changed Subject"
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])

        assert len(history) == 1

        retrieved = test_db.get_all_emails()[0]
        assert retrieved["subject"] == "Changed Subject"

    def test_from_field_change_not_tracked(self, test_db, sample_email):
        """Test that from/to changes are not tracked"""
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["from"] = "different@example.com"
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 1
