import pytest
import time
from datetime import datetime, timedelta, timezone
from src.rule_engine import RuleEngine
from src.rule_validator import RuleValidationError


class TestIntegration:
    def test_complete_workflow_mock(
        self, test_db, temp_rules_file, sample_emails_batch
    ):
        """Test complete workflow with mocked Gmail"""
        emails = sample_emails_batch

        test_db.insert_emails_batch(emails)

        assert test_db.count_emails() == len(emails)

        rules = {
            "rules": [
                {
                    "name": "Mark all as read",
                    "predicate": "any",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "sender"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        engine = RuleEngine(rules_file)

        stored_emails = test_db.get_all_emails()
        matched_count = 0

        for email in stored_emails:
            actions = engine.evaluate_rules(email)
            if actions:
                matched_count += 1

        assert matched_count == len(emails)

    def test_temporal_tracking_during_processing(
        self, test_db, temp_rules_file, sample_email
    ):
        """Test that processing creates temporal versions"""
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["Work"]
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 2
        assert history[0]["is_current"] == False
        assert history[1]["is_current"] == True

        assert history[1]["labels"] == ["Work"]

    def test_invalid_rules_stop_processing(
        self, test_db, temp_rules_file, sample_emails_batch
    ):
        """Test that invalid rules prevent processing"""
        test_db.insert_emails_batch(sample_emails_batch)

        rules = {
            "rules": [
                {
                    "name": "Invalid",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "invalid_field",  # Invalid
                            "predicate": "contains",
                            "value": "test",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        with pytest.raises(RuleValidationError):
            engine = RuleEngine(rules_file)

    def test_incremental_fetch_simulation(self, test_db, sample_emails_batch):
        """Test incremental fetch logic"""
        first_batch = sample_emails_batch[:5]
        test_db.insert_emails_batch(first_batch)

        stored_ids = test_db.get_stored_email_ids()
        assert len(stored_ids) == 5

        second_batch = sample_emails_batch

        new_emails = [e for e in second_batch if e["id"] not in stored_ids]
        assert len(new_emails) == 5

        test_db.insert_emails_batch(new_emails)

        assert test_db.count_emails() == 10

    def test_process_same_emails_multiple_times(
        self, test_db, temp_rules_file, sample_emails_batch
    ):
        """Test processing same batch multiple times"""
        test_db.insert_emails_batch(sample_emails_batch)

        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "sender"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        emails = test_db.get_all_emails()
        for email in emails:
            engine.evaluate_rules(email)

        emails = test_db.get_all_emails()
        for email in emails:
            actions = engine.evaluate_rules(email)
            assert len(actions) > 0

    def test_exclusive_move_behavior(self, test_db, sample_email):
        """
        Test exclusive move behavior (folder-style)
        Moving to new label removes previous user labels
        """
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["UNREAD", "Work"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        current = test_db.get_email_by_id(sample_email["id"])
        assert "Work" in current["labels"]
        assert "INBOX" not in current["labels"]
        assert "UNREAD" in current["labels"]

        sample_email["labels"] = ["UNREAD", "Important"]
        test_db.insert_or_update_email(sample_email)

        current = test_db.get_email_by_id(sample_email["id"])
        assert "Important" in current["labels"]
        assert "Work" not in current["labels"]
        assert "INBOX" not in current["labels"]
        assert "UNREAD" in current["labels"]

    def test_sequential_moves_create_history(self, test_db, sample_email):
        """Test that sequential moves create temporal history"""
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["UNREAD", "Personal"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["UNREAD", "Work"]
        test_db.insert_or_update_email(sample_email)
        time.sleep(0.01)

        sample_email["labels"] = ["Archive"]
        test_db.insert_or_update_email(sample_email)

        history = test_db.get_email_history(sample_email["id"])
        assert len(history) == 4

        current_versions = [v for v in history if v["is_current"]]
        assert len(current_versions) == 1
        assert "Archive" in current_versions[0]["labels"]

        assert "INBOX" in history[0]["labels"]  # Original
        assert "Personal" in history[1]["labels"]  # First move
        assert "Work" in history[2]["labels"]  # Second move
        assert "Archive" in history[3]["labels"]  # Final

    def test_system_labels_preserved_during_moves(self, test_db):
        """Test that system state labels (UNREAD, STARRED) are preserved"""
        email = {
            "id": "test_system_preserve",
            "from": "test@example.com",
            "subject": "Important Email",
            "message": "Test content",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD", "STARRED", "IMPORTANT", "Personal"],
        }

        test_db.insert_or_update_email(email)
        time.sleep(0.01)

        email["labels"] = ["UNREAD", "STARRED", "IMPORTANT", "Work"]
        test_db.insert_or_update_email(email)

        current = test_db.get_email_by_id(email["id"])

        assert "UNREAD" in current["labels"]
        assert "STARRED" in current["labels"]
        assert "IMPORTANT" in current["labels"]

        assert "Work" in current["labels"]
        assert "Personal" not in current["labels"]
        assert "INBOX" not in current["labels"]

    def test_multiple_rules_multiple_moves(self, test_db, temp_rules_file):
        """
        Test multiple matching rules with move actions
        """
        email = {
            "id": "test_multi_move",
            "from": "boss@company.com",
            "subject": "Urgent Project",
            "message": "This is important",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD"],
        }

        test_db.insert_or_update_email(email)

        rules = {
            "rules": [
                {
                    "name": "From Boss",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "boss"}
                    ],
                    "actions": [{"type": "move_message", "destination": "FromBoss"}],
                },
                {
                    "name": "Urgent",
                    "predicate": "all",
                    "conditions": [
                        {"field": "subject", "predicate": "contains", "value": "Urgent"}
                    ],
                    "actions": [{"type": "move_message", "destination": "Urgent"}],
                },
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        actions = engine.evaluate_rules(email)

        move_actions = [a for a in actions if a["type"] == "move_message"]
        assert len(move_actions) == 2
        assert move_actions[0]["destination"] == "FromBoss"
        assert move_actions[1]["destination"] == "Urgent"

    def test_rule_with_all_predicate(
        self, test_db, temp_rules_file, sample_emails_batch
    ):
        """Test rule with ALL predicate (AND logic)"""
        test_db.insert_emails_batch(sample_emails_batch)

        rules = {
            "rules": [
                {
                    "name": "Specific Match",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "sender0"},
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "Subject 0",
                        },
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        emails = test_db.get_all_emails()
        matched = 0
        for email in emails:
            actions = engine.evaluate_rules(email)
            if actions:
                matched += 1

        assert matched == 1

    def test_rule_with_any_predicate(
        self, test_db, temp_rules_file, sample_emails_batch
    ):
        """Test rule with ANY predicate (OR logic)"""
        test_db.insert_emails_batch(sample_emails_batch)

        rules = {
            "rules": [
                {
                    "name": "Broad Match",
                    "predicate": "any",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "sender0"},
                        {"field": "from", "predicate": "contains", "value": "sender1"},
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        emails = test_db.get_all_emails()
        matched = 0
        for email in emails:
            actions = engine.evaluate_rules(email)
            if actions:
                matched += 1

        assert matched == 2

    def test_date_based_rules(self, test_db, temp_rules_file):
        """Test rules with date conditions"""
        old_email = {
            "id": "old_email",
            "from": "test@example.com",
            "subject": "Old",
            "message": "Old email",
            "received_date": datetime.now() - timedelta(days=40),
            "labels": ["INBOX"],
        }

        recent_email = {
            "id": "recent_email",
            "from": "test@example.com",
            "subject": "Recent",
            "message": "Recent email",
            "received_date": datetime.now() - timedelta(days=5),
            "labels": ["INBOX"],
        }

        test_db.insert_or_update_email(old_email)
        test_db.insert_or_update_email(recent_email)

        rules = {
            "rules": [
                {
                    "name": "Archive Old",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "30",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "move_message", "destination": "Archive"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        old_actions = engine.evaluate_rules(old_email)
        assert len(old_actions) == 0

        recent_actions = engine.evaluate_rules(recent_email)
        assert len(recent_actions) > 0

    def test_complex_rule_combination(self, test_db, temp_rules_file):
        """Test complex rule with multiple conditions and actions"""
        email = {
            "id": "complex_test",
            "from": "boss@company.com",
            "subject": "Urgent: Interview Candidate",
            "message": "Please review ASAP",
            "received_date": datetime.now() - timedelta(hours=2),
            "labels": ["INBOX", "UNREAD"],
        }

        test_db.insert_or_update_email(email)

        rules = {
            "rules": [
                {
                    "name": "Urgent Interview from Boss",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "boss"},
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "Urgent",
                        },
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "Interview",
                        },
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "1",
                            "unit": "days",
                        },
                    ],
                    "actions": [
                        {"type": "mark_as_unread"},
                        {"type": "move_message", "destination": "HighPriority"},
                    ],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        actions = engine.evaluate_rules(email)

        assert len(actions) == 2
        assert any(a["type"] == "mark_as_unread" for a in actions)
        assert any(a["type"] == "move_message" for a in actions)

    def test_large_email_batch(self, test_db):
        """Test handling large number of emails"""
        large_batch = [
            {
                "id": f"email_{i:04d}",
                "from": f"sender{i}@example.com",
                "subject": f"Subject {i}",
                "message": f"Message {i}",
                "received_date": datetime.now() - timedelta(days=i % 365),
                "labels": ["INBOX"],
            }
            for i in range(1000)
        ]

        test_db.insert_emails_batch(large_batch)
        assert test_db.count_emails() == 1000

        all_emails = test_db.get_all_emails()
        assert len(all_emails) == 1000

    def test_empty_rules_file(self, test_db, temp_rules_file):
        """Test handling empty rules file"""
        rules = {"rules": []}
        rules_file = temp_rules_file(rules)

        engine = RuleEngine(rules_file)

        email = {"from": "test@example.com", "subject": "Test"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0


class TestEdgeCases:
    """Edge cases and potential gaps in implementation (class-wrapped)"""

    def test_sql_injection_attempt(self, test_db):
        malicious_email = {
            "id": "test'; DROP TABLE emails; --",
            "from": "'; DELETE FROM emails WHERE '1'='1",
            "subject": "Test",
            "message": "Test",
            "received_date": datetime.now(),
            "labels": [],
        }
        test_db.insert_or_update_email(malicious_email)
        emails = test_db.get_all_emails()
        assert len(emails) == 1

    def test_malformed_json_in_labels(self, test_db):
        email = {
            "id": "test_json",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", 'Label with "quotes"', "Label with 'apostrophe'"],
        }
        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()[0]
        assert len(retrieved["labels"]) == 3

    def test_concurrent_email_updates(self, test_db, sample_email):
        import threading

        def update_email(label_name):
            email = sample_email.copy()
            email["labels"] = [label_name]
            test_db.insert_or_update_email(email)

        threads = []
        for i in range(5):
            t = threading.Thread(target=update_email, args=(f"LABEL_{i}",))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        history = test_db.get_email_history(sample_email["id"])
        assert len(history) >= 1

    def test_extremely_long_rule_name(self, temp_rules_file):
        rules = {
            "rules": [
                {
                    "name": "A" * 10000,
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_validator import RuleValidator, RuleValidationError

        validator = RuleValidator()
        try:
            result = validator.validate_rules_file(rules_file)
            if not result:
                assert len(validator.errors) > 0
        except RuleValidationError:
            pass

    def test_rule_with_whitespace_only_value(self, temp_rules_file):
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "   "}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_engine import RuleEngine

        engine = RuleEngine(rules_file)
        email = {"from": "test@example.com"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0

    def test_gmail_id_uniqueness_assumption(self, test_db):
        email1 = {
            "id": "same_id_123",
            "from": "sender1@example.com",
            "subject": "Subject 1",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        email2 = {
            "id": "same_id_123",
            "from": "sender2@example.com",
            "subject": "Subject 2",
            "received_date": datetime.now(),
            "labels": ["IMPORTANT"],
        }
        test_db.insert_or_update_email(email1)
        test_db.insert_or_update_email(email2)
        emails = test_db.get_all_emails()
        assert len(emails) == 1
        current = emails[0]
        assert current["from"] in ["sender1@example.com", "sender2@example.com"]

    def test_partial_batch_insert_failure(self, test_db):
        emails = [
            {
                "id": "good_email_1",
                "from": "test@example.com",
                "subject": "Good",
                "received_date": datetime.now(),
                "labels": ["INBOX"],
            },
            {
                "id": "bad_email",
                "from": None,
                "subject": None,
                "message": "",
                "received_date": "invalid_date",
                "labels": None,
            },
            {
                "id": "good_email_2",
                "from": "test2@example.com",
                "subject": "Good",
                "received_date": datetime.now(),
                "labels": ["INBOX"],
            },
        ]
        test_db.insert_emails_batch(emails)
        count = test_db.count_emails()
        assert count >= 2

    def test_extremely_large_email_message(self, test_db):
        large_message = "A" * (10 * 1024 * 1024)
        email = {
            "id": "large_email",
            "from": "test@example.com",
            "subject": "Large",
            "message": large_message,
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(email)
        retrieved = test_db.get_all_emails()[0]
        assert len(retrieved["message"]) == len(large_message)

    def test_regex_patterns_in_rule_values(self, temp_rules_file):
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "from",
                            "predicate": "contains",
                            "value": ".*@example\\.com",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_engine import RuleEngine

        engine = RuleEngine(rules_file)
        email1 = {"from": "user@example.com"}
        actions1 = engine.evaluate_rules(email1)
        assert len(actions1) == 0
        email2 = {"from": "test.*@example\\.com"}
        actions2 = engine.evaluate_rules(email2)
        assert len(actions2) > 0

    def test_mixed_timezone_dates(self, test_db):
        email1 = {
            "id": "email_utc",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(timezone.utc),
            "labels": ["INBOX"],
        }
        email2 = {
            "id": "email_naive",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(email1)
        test_db.insert_or_update_email(email2)
        emails = test_db.get_all_emails()
        assert len(emails) == 2

    def test_move_to_label_with_special_characters(self, temp_rules_file):
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [
                        {"type": "move_message", "destination": "Work/Projects/2024"}
                    ],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_validator import RuleValidator

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)
        assert result is True

    def test_email_fetch_order_independence(self, test_db):
        base_time = datetime.now()
        emails_newest_first = [
            {
                "id": f"email_{i}",
                "from": "test@example.com",
                "subject": f"Email {i}",
                "received_date": base_time - timedelta(days=i),
                "labels": ["INBOX"],
            }
            for i in range(10)
        ]
        db1 = test_db
        for email in emails_newest_first:
            db1.insert_or_update_email(email)
        count = db1.count_emails()
        assert count == 10

    def test_empty_vs_no_rules(self, temp_rules_file):
        from src.rule_engine import RuleEngine

        rules1 = {"rules": []}
        file1 = temp_rules_file(rules1)
        engine1 = RuleEngine(file1)
        email = {"from": "test@example.com"}
        actions = engine1.evaluate_rules(email)
        assert actions == []

    def test_very_old_email_dates(self, test_db):
        old_email = {
            "id": "very_old",
            "from": "test@example.com",
            "subject": "Old",
            "received_date": datetime(1970, 1, 1),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(old_email)
        retrieved = test_db.get_all_emails()
        assert len(retrieved) == 1
        assert retrieved[0]["received_date"].year == 1970

    def test_future_email_dates(self, test_db, temp_rules_file):
        future_email = {
            "id": "future",
            "from": "test@example.com",
            "subject": "Future",
            "received_date": datetime.now() + timedelta(days=365),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(future_email)
        rules = {
            "rules": [
                {
                    "name": "Old emails",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "7",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_engine import RuleEngine

        engine = RuleEngine(rules_file)
        actions = engine.evaluate_rules(future_email)
        assert len(actions) > 0

    def test_case_sensitivity_consistency(self, temp_rules_file):
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "from",
                            "predicate": "equals",
                            "value": "TEST@EXAMPLE.COM",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        from src.rule_engine import RuleEngine

        engine = RuleEngine(rules_file)
        variations = [
            "test@example.com",
            "TEST@EXAMPLE.COM",
            "Test@Example.Com",
            "TeSt@ExAmPlE.cOm",
        ]
        for email_addr in variations:
            email = {"from": email_addr}
            actions = engine.evaluate_rules(email)
            assert len(actions) > 0, f"Failed for: {email_addr}"

    def test_exclusive_move_removes_previous_user_labels(self, test_db):
        email = {
            "id": "test_exclusive_move",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD", "Personal"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["UNREAD", "Work"]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "Work" in current["labels"]
        assert "Personal" not in current["labels"]
        assert "INBOX" not in current["labels"]
        assert "UNREAD" in current["labels"]

    def test_system_labels_preserved_during_move(self, test_db):
        email = {
            "id": "test_system_labels",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD", "STARRED", "IMPORTANT", "Work"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["UNREAD", "STARRED", "IMPORTANT", "Projects"]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "UNREAD" in current["labels"]
        assert "STARRED" in current["labels"]
        assert "IMPORTANT" in current["labels"]
        assert "Projects" in current["labels"]
        assert "Work" not in current["labels"]
        assert "INBOX" not in current["labels"]

    def test_move_to_same_label_idempotent(self, test_db):
        email = {
            "id": "test_idempotent",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["Work", "UNREAD"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["Work", "UNREAD"]
        test_db.insert_or_update_email(email)
        history = test_db.get_email_history(email["id"])
        assert len(history) == 1

    def test_multiple_sequential_moves(self, test_db):
        email = {
            "id": "test_sequential_moves",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["UNREAD", "Personal"]
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        current = test_db.get_email_by_id(email["id"])
        assert "Personal" in current["labels"]
        assert "INBOX" not in current["labels"]
        email["labels"] = ["UNREAD", "Work"]
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        current = test_db.get_email_by_id(email["id"])
        assert "Work" in current["labels"]
        assert "Personal" not in current["labels"]
        assert "INBOX" not in current["labels"]
        email["labels"] = ["Archive"]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "Archive" in current["labels"]
        assert "Work" not in current["labels"]
        assert "Personal" not in current["labels"]
        assert "UNREAD" not in current["labels"]
        history = test_db.get_email_history(email["id"])
        assert len(history) == 4

    def test_inbox_always_removed_on_move(self, test_db):
        email = {
            "id": "test_inbox_removal",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD", "Personal"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["UNREAD", "Work"]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "INBOX" not in current["labels"]
        assert "Work" in current["labels"]

    def test_temporal_history_tracks_all_moves(self, test_db):
        email = {
            "id": "test_move_history",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        moves = ["Personal", "Work", "Projects", "Archive"]
        for label in moves:
            email["labels"] = [label]
            test_db.insert_or_update_email(email)
            time.sleep(0.01)
        history = test_db.get_email_history(email["id"])
        assert len(history) == 5
        current_versions = [v for v in history if v["is_current"]]
        assert len(current_versions) == 1
        assert "Archive" in current_versions[0]["labels"]
        assert "INBOX" in history[0]["labels"]
        assert "Personal" in history[1]["labels"]
        assert "Work" in history[2]["labels"]
        assert "Projects" in history[3]["labels"]
        assert "Archive" in history[4]["labels"]

    def test_move_with_multiple_system_labels(self, test_db):
        email = {
            "id": "test_multiple_system",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX", "UNREAD", "STARRED", "IMPORTANT", "CATEGORY_PERSONAL"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = [
            "UNREAD",
            "STARRED",
            "IMPORTANT",
            "CATEGORY_PERSONAL",
            "Work",
        ]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "UNREAD" in current["labels"]
        assert "STARRED" in current["labels"]
        assert "IMPORTANT" in current["labels"]
        assert "CATEGORY_PERSONAL" in current["labels"]
        assert "Work" in current["labels"]
        assert "INBOX" not in current["labels"]

    def test_move_from_sent_folder(self, test_db):
        email = {
            "id": "test_sent_move",
            "from": "me@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["SENT", "UNREAD"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = ["Archive"]
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert "SENT" not in current["labels"]
        assert "Archive" in current["labels"]

    def test_rapid_moves_all_tracked(self, test_db):
        email = {
            "id": "test_rapid",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(email)
        for i in range(10):
            email["labels"] = [f"Label_{i}"]
            test_db.insert_or_update_email(email)
        history = test_db.get_email_history(email["id"])
        assert len(history) == 11
        timestamps = [h["valid_from"] for h in history]
        assert len(set(timestamps)) == 11

    def test_empty_labels_after_move(self, test_db):
        email = {
            "id": "test_empty_labels",
            "from": "test@example.com",
            "subject": "Test",
            "received_date": datetime.now(),
            "labels": ["INBOX"],
        }
        test_db.insert_or_update_email(email)
        time.sleep(0.01)
        email["labels"] = []
        test_db.insert_or_update_email(email)
        current = test_db.get_email_by_id(email["id"])
        assert current["labels"] == []
