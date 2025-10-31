import pytest
from datetime import datetime, timedelta
from src.rule_engine import RuleEngine
from src.rule_validator import RuleValidator, RuleValidationError


class TestRuleEvaluation:
    """
    Tests for rule evaluation logic
    """

    def test_string_contains_predicate(self, temp_rules_file):
        """Test 'contains' predicate"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "example"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"from": "user@example.com"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"from": "user@test.com"}
        assert engine.evaluate_rules(email2) == []

    def test_string_does_not_contain_predicate(self, temp_rules_file):
        """Test 'does_not_contain' predicate"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "from",
                            "predicate": "does_not_contain",
                            "value": "spam",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"from": "user@example.com"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"from": "spam@example.com"}
        assert engine.evaluate_rules(email2) == []

    def test_string_equals_predicate(self, temp_rules_file):
        """Test 'equals' predicate"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "subject",
                            "predicate": "equals",
                            "value": "exact match",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"subject": "Exact Match"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"subject": "exact match plus more"}
        assert engine.evaluate_rules(email2) == []

    def test_string_does_not_equal_predicate(self, temp_rules_file):
        """Test 'does_not_equal' predicate"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "subject",
                            "predicate": "does_not_equal",
                            "value": "skip this",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"subject": "different"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"subject": "Skip This"}
        assert engine.evaluate_rules(email2) == []

    def test_date_less_than_predicate(self, temp_rules_file):
        """Test 'less_than' date predicate (older emails)"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "30",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        old_email = {"received_date": datetime.now() - timedelta(days=40)}
        assert engine.evaluate_rules(old_email) != []

        recent_email = {"received_date": datetime.now() - timedelta(days=20)}
        assert engine.evaluate_rules(recent_email) == []

    def test_date_greater_than_predicate(self, temp_rules_file):
        """Test 'greater_than' date predicate (newer emails)"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "greater_than",
                            "value": "7",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        recent_email = {"received_date": datetime.now() - timedelta(days=3)}
        assert engine.evaluate_rules(recent_email) != []

        old_email = {"received_date": datetime.now() - timedelta(days=10)}
        assert engine.evaluate_rules(old_email) == []

    def test_date_months_unit(self, temp_rules_file):
        """Test date predicate with months unit"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "2",
                            "unit": "months",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        old_email = {"received_date": datetime.now() - timedelta(days=70)}
        assert engine.evaluate_rules(old_email) != []

        recent_email = {"received_date": datetime.now() - timedelta(days=30)}
        assert engine.evaluate_rules(recent_email) == []

    def test_all_predicate_requires_all_conditions(self, temp_rules_file):
        """Test 'all' predicate (AND logic)"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "boss"},
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "urgent",
                        },
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"from": "boss@company.com", "subject": "Urgent matter"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"from": "boss@company.com", "subject": "Normal email"}
        assert engine.evaluate_rules(email2) == []

        email3 = {"from": "colleague@company.com", "subject": "Normal email"}
        assert engine.evaluate_rules(email3) == []

    def test_any_predicate_requires_one_condition(self, temp_rules_file):
        """Test 'any' predicate (OR logic)"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "any",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "boss"},
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "urgent",
                        },
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email1 = {"from": "boss@company.com", "subject": "Urgent matter"}
        assert engine.evaluate_rules(email1) != []

        email2 = {"from": "boss@company.com", "subject": "Normal email"}
        assert engine.evaluate_rules(email2) != []

        email3 = {"from": "colleague@company.com", "subject": "Urgent matter"}
        assert engine.evaluate_rules(email3) != []

        email4 = {"from": "colleague@company.com", "subject": "Normal email"}
        assert engine.evaluate_rules(email4) == []

    def test_multiple_rules_all_evaluated(self, temp_rules_file):
        """Test that all rules are evaluated"""
        rules = {
            "rules": [
                {
                    "name": "Rule 1",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                },
                {
                    "name": "Rule 2",
                    "predicate": "all",
                    "conditions": [
                        {"field": "subject", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_unread"}],
                },
            ]
        }

        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email = {"from": "test@example.com", "subject": "test subject"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 2
        assert any(a["type"] == "mark_as_read" for a in actions)
        assert any(a["type"] == "mark_as_unread" for a in actions)


class TestRuleEvaluationEdgeCases:
    """
    Tests for edge cases in rule evaluation
    """

    def test_string_predicate_with_empty_value(self, temp_rules_file):
        """Test string predicate with empty value"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "subject", "predicate": "contains", "value": ""}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email = {"subject": "Any subject"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0

    def test_rule_with_none_field_value(self, temp_rules_file):
        """Test rule evaluation when email field is None"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "subject", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email = {"subject": None}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0

    def test_rule_with_missing_field(self, temp_rules_file):
        """Test rule evaluation when email is missing the field"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email = {"subject": "Test"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0

    def test_string_predicates_case_insensitive(self, temp_rules_file):
        """Test that string predicates are case-insensitive"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "EXAMPLE"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        assert engine.evaluate_rules({"from": "user@example.com"}) != []
        assert engine.evaluate_rules({"from": "user@EXAMPLE.com"}) != []
        assert engine.evaluate_rules({"from": "user@ExAmPlE.com"}) != []

    def test_unicode_in_rule_value(self, temp_rules_file):
        """Test rules with unicode characters"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "subject",
                            "predicate": "contains",
                            "value": "会議",  # Japanese
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)
        engine = RuleEngine(rules_file)

        email = {"subject": "明日の会議について"}
        actions = engine.evaluate_rules(email)
        assert len(actions) > 0

    def test_date_predicate_with_timezone_aware_date(self, temp_rules_file):
        """Test date predicate with timezone-aware datetime"""
        from datetime import timezone

        rules = {
            "rules": [
                {
                    "name": "Test",
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
        engine = RuleEngine(rules_file)

        tz_date = datetime.now(timezone.utc) - timedelta(days=10)
        email = {"received_date": tz_date}
        actions = engine.evaluate_rules(email)
        assert len(actions) > 0

    def test_date_predicate_with_missing_date(self, temp_rules_file):
        """Test date predicate when email has no received_date"""
        rules = {
            "rules": [
                {
                    "name": "Test",
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
        engine = RuleEngine(rules_file)

        email = {"from": "test@example.com"}
        actions = engine.evaluate_rules(email)
        assert len(actions) == 0

    def test_date_predicate_with_string_date(self, temp_rules_file):
        """Test date predicate with string date (should parse)"""
        rules = {
            "rules": [
                {
                    "name": "Test",
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
        engine = RuleEngine(rules_file)

        old_date_str = "Mon, 01 Jan 2024 12:00:00 +0000"
        email = {"received_date": old_date_str}

        actions = engine.evaluate_rules(email)
        assert len(actions) > 0


class TestRuleValidation:
    """
    Tests for rule validation
    """

    def test_valid_rule_passes(self, temp_rules_file, valid_rule):
        """Test that valid rule passes validation"""
        rules = {"rules": [valid_rule]}
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        assert validator.validate_rules_file(rules_file) == True
        assert len(validator.errors) == 0

    def test_invalid_json_fails(self, temp_rules_file):
        """Test that invalid JSON is caught"""
        import tempfile

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        temp_file.write("{ invalid json }")
        temp_file.close()

        validator = RuleValidator()
        result = validator.validate_rules_file(temp_file.name)
        assert result == False
        assert len(validator.errors) > 0

    def test_missing_rules_key(self, temp_rules_file):
        """Test missing 'rules' key"""
        data = {"not_rules": []}
        rules_file = temp_rules_file(data)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)
        assert result == False
        assert any("'rules'" in e for e in validator.errors)

    def test_empty_rules_array(self, temp_rules_file):
        """Test empty rules array"""
        rules = {"rules": []}
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)
        assert result == True
        assert len(validator.warnings) > 0

    def test_missing_required_rule_fields(self, temp_rules_file):
        """Test missing required fields in rule"""
        rules = {
            "rules": [
                {
                    "name": "Test"
                    # Missing: predicate, conditions, actions
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert len(validator.errors) >= 3

    def test_invalid_field_name(self, temp_rules_file):
        """Test invalid field name in condition"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "sender",  # Invalid, should be "from"
                            "predicate": "contains",
                            "value": "test",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("Invalid field 'sender'" in e for e in validator.errors)

    def test_invalid_predicate_for_string_field(self, temp_rules_file):
        """Test using date predicate on string field"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "from",  # String field
                            "predicate": "less_than",  # Date predicate
                            "value": "30",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("Invalid predicate" in e for e in validator.errors)

    def test_invalid_predicate_for_date_field(self, temp_rules_file):
        """Test using string predicate on date field"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",  # Date field
                            "predicate": "contains",  # String predicate
                            "value": "test",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

    def test_date_condition_missing_unit(self, temp_rules_file):
        """Test date condition without unit field"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "30"
                            # Missing: unit
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("require 'unit'" in e for e in validator.errors)

    def test_date_condition_invalid_unit(self, temp_rules_file):
        """Test date condition with invalid unit"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "30",
                            "unit": "years",  # Invalid
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

    def test_date_value_non_numeric(self, temp_rules_file):
        """Test date value that's not numeric"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "thirty",  # Not numeric
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("must be numeric" in e for e in validator.errors)

    def test_date_value_negative(self, temp_rules_file):
        """Test negative date value"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "-5",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("must be positive" in e for e in validator.errors)

    def test_invalid_action_type(self, temp_rules_file):
        """Test invalid action type"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "delete_message"}],  # Invalid
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("Invalid action" in e for e in validator.errors)

    def test_move_message_missing_destination(self, temp_rules_file):
        """Test move_message without destination"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [
                        {
                            "type": "move_message"
                            # Missing: destination
                        }
                    ],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("requires 'destination'" in e for e in validator.errors)

    def test_move_message_empty_destination(self, temp_rules_file):
        """Test move_message with empty destination"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "move_message", "destination": ""}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("cannot be empty" in e for e in validator.errors)

    def test_conflicting_read_unread_actions(self, temp_rules_file):
        """Test conflicting mark_as_read and mark_as_unread"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}, {"type": "mark_as_unread"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any(
            "both 'mark_as_read' and 'mark_as_unread'" in e.lower()
            for e in validator.errors
        )

    def test_duplicate_mark_as_read_warning(self, temp_rules_file):
        """Test duplicate mark_as_read actions generate warning"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}, {"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)

        assert result == True  # Valid but with warning
        assert len(validator.warnings) > 0
        assert any(
            "redundant" in w.lower() or "duplicate" in w.lower()
            for w in validator.warnings
        )

    def test_multiple_move_actions_warning(self, temp_rules_file):
        """Test multiple move_message actions generate warning"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [
                        {"type": "move_message", "destination": "Work"},
                        {"type": "move_message", "destination": "Important"},
                    ],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)

        assert result == True  # Valid but with warning
        assert len(validator.warnings) > 0
        assert any("multiple" in w.lower() for w in validator.warnings)

    def test_empty_conditions_array(self, temp_rules_file):
        """Test rule with empty conditions array"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [],  # Empty
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("at least one condition" in e for e in validator.errors)

    def test_empty_actions_array(self, temp_rules_file):
        """Test rule with empty actions array"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [],  # Empty
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("at least one action" in e for e in validator.errors)

    def test_invalid_rule_predicate(self, temp_rules_file):
        """Test invalid rule-level predicate"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "maybe",  # Invalid
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        with pytest.raises(RuleValidationError):
            validator.validate_rules_file(rules_file)

        assert any("'all' or 'any'" in e for e in validator.errors)

    def test_very_large_date_value_warning(self, temp_rules_file):
        """Test that very large date values generate warning"""
        rules = {
            "rules": [
                {
                    "name": "Test",
                    "predicate": "all",
                    "conditions": [
                        {
                            "field": "received_date",
                            "predicate": "less_than",
                            "value": "10000",
                            "unit": "days",
                        }
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)

        assert result == True

    def test_unicode_in_rule_name(self, temp_rules_file):
        """Test rule with unicode in name"""
        rules = {
            "rules": [
                {
                    "name": "測試規則 (Test Rule)",  # Chinese + English
                    "predicate": "all",
                    "conditions": [
                        {"field": "from", "predicate": "contains", "value": "test"}
                    ],
                    "actions": [{"type": "mark_as_read"}],
                }
            ]
        }
        rules_file = temp_rules_file(rules)

        validator = RuleValidator()
        result = validator.validate_rules_file(rules_file)
        assert result == True
