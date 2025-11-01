"""
Rule engine for evaluating email rules
Automatically validates rules before loading
"""
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dateutil import parser as date_parser
from src.rule_validator import RuleValidator, RuleValidationError


class RuleEngine:
    def __init__(self, rules_file="rules.json"):
        """Initialize rule engine with validation"""
        # Validate rules before loading
        self._validate_rules(rules_file)
        self.rules = self.load_rules(rules_file)
        print(f"Loaded {len(self.rules)} validated rules")

    def _validate_rules(self, rules_file: str):
        """Validate rules file - integrated validation"""
        print(f"Validating rules file: {rules_file}")
        validator = RuleValidator()

        try:
            validator.validate_rules_file(rules_file)

            if validator.warnings:
                print(f"\nValidation warnings:")
                for warning in validator.warnings:
                    print(f"  {warning}")
                print()

        except RuleValidationError as e:
            print(f"\nRule validation failed:")
            print(str(e))
            print("\nFix the errors above and try again.")
            raise

    def load_rules(self, rules_file: str) -> List[Dict]:
        """Load rules from JSON file (already validated)"""
        try:
            with open(rules_file, "r") as f:
                data = json.load(f)
            return data.get("rules", [])
        except Exception as e:
            print(f"Error loading rules: {e}")
            return []

    def evaluate_rules(self, email: Dict) -> List[Dict]:
        """Evaluate all rules against an email"""
        actions_to_execute = []

        for rule in self.rules:
            if self.evaluate_rule(rule, email):
                print(f"Rule matched: '{rule['name']}'")
                actions_to_execute.extend(rule["actions"])

        return actions_to_execute

    def evaluate_rule(self, rule: Dict, email: Dict) -> bool:
        """Evaluate a single rule"""
        predicate = rule["predicate"].lower()
        conditions = rule["conditions"]

        results = [self.evaluate_condition(cond, email) for cond in conditions]

        if predicate == "all":
            return all(results)
        elif predicate == "any":
            return any(results)
        return False

    def evaluate_condition(self, condition: Dict, email: Dict) -> bool:
        """Evaluate a single condition"""
        field = condition["field"]
        predicate = condition["predicate"]
        value = condition["value"]

        email_value = email.get(field, "")

        if field == "received_date":
            unit = condition.get("unit", "days")
            return self._evaluate_date_condition(email_value, predicate, value, unit)
        else:
            return self._evaluate_string_condition(email_value, predicate, value)

    def _evaluate_string_condition(
        self, email_value: str, predicate: str, condition_value: str
    ) -> bool:
        """Evaluate string-based conditions"""
        if email_value is None:
            email_value = ""

        if condition_value is None:
            return False

        raw_email_value = str(email_value).lower()
        email_value = raw_email_value.strip()
        condition_value = str(condition_value).lower().strip()

        if condition_value == "":
            return False
        if predicate == "contains":
            return condition_value in email_value
        elif predicate == "does_not_contain":
            return condition_value not in email_value
        elif predicate == "equals":
            return email_value == condition_value
        elif predicate == "does_not_equal":
            return email_value != condition_value
        return False

    def _evaluate_date_condition(
        self, email_date: Any, predicate: str, value: str, unit: str
    ) -> bool:
        """Evaluate date-based conditions"""
        if not email_date:
            return False

        try:
            if isinstance(email_date, str):
                email_date = date_parser.parse(email_date)

            now = datetime.now()
            if email_date.tzinfo is not None:
                from datetime import timezone

                now = datetime.now(timezone.utc)
                email_date = email_date.astimezone(timezone.utc)

            value_int = int(value)

            if unit == "days":
                threshold = now - timedelta(days=value_int)
            elif unit == "months":
                threshold = now - timedelta(days=value_int * 30)
            else:
                return False

            if predicate == "less_than":
                return email_date > threshold
            elif predicate == "greater_than":
                return email_date < threshold

        except Exception as e:
            print(f"Error evaluating date: {e}")
            return False

        return False
