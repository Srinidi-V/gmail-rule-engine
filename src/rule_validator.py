"""
Rule validator for Gmail rule engine
Validates rules based on assignment requirements
"""
from typing import Dict, List
import json

class RuleValidationError(Exception):
    """Custom exception for rule validation errors"""
    pass

class RuleValidator:
    """Validates rules against assignment requirements"""
    
    # Assignment requirements
    SUPPORTED_FIELDS = {
        'from': 'string',          # From field
        'subject': 'string',       # Subject field
        'message': 'string',       # Message field
        'received_date': 'date'    # Date Received field
    }
    
    # String predicates from assignment
    STRING_PREDICATES = [
        'contains',
        'does_not_contain',
        'equals',
        'does_not_equal'
    ]
    
    # Date predicates from assignment
    DATE_PREDICATES = [
        'less_than',     # Less than X days/months
        'greater_than'   # Greater than X days/months
    ]
    
    # Rule-level predicates from assignment
    RULE_PREDICATES = ['all', 'any']
    
    # Actions from assignment
    SUPPORTED_ACTIONS = [
        'mark_as_read',
        'mark_as_unread',
        'move_message'
    ]
    
    VALID_DATE_UNITS = ['days', 'months']
    
    # Reasonable limits
    MAX_CONDITIONS = 10
    MAX_ACTIONS = 5
    MAX_RULES = 100
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_rules_file(self, rules_file: str) -> bool:
        """Validate entire rules file"""
        self.errors = []
        self.warnings = []
        
        try:
            with open(rules_file, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            self._add_error(f"Rules file not found: {rules_file}")
            return False
        except json.JSONDecodeError as e:
            self._add_error(f"Invalid JSON: {e}")
            return False
        
        if not isinstance(data, dict) or 'rules' not in data:
            self._add_error("Rules file must contain 'rules' array")
            return False
        
        rules = data['rules']
        
        if not isinstance(rules, list):
            self._add_error("'rules' must be an array")
            return False
        
        if len(rules) == 0:
            self._add_warning("No rules defined")
            return True
        
        if len(rules) > self.MAX_RULES:
            self._add_error(f"Too many rules: {len(rules)} (max: {self.MAX_RULES})")
        
        for i, rule in enumerate(rules):
            self._validate_rule(rule, i)
        
        if self.errors:
            raise RuleValidationError(self._format_errors())
        
        return True
    
    def _validate_rule(self, rule: Dict, rule_index: int):
        """Validate a single rule"""
        rule_name = rule.get('name', f'Rule #{rule_index + 1}')
        
        # Check required fields
        for field in ['name', 'predicate', 'conditions', 'actions']:
            if field not in rule:
                self._add_error(f"Rule '{rule_name}': Missing '{field}'")
        
        # Validate predicate
        if 'predicate' in rule:
            if rule['predicate'].lower() not in self.RULE_PREDICATES:
                self._add_error(
                    f"Rule '{rule_name}': Invalid predicate '{rule['predicate']}'. "
                    f"Must be 'all' or 'any'"
                )
        
        # Validate conditions
        if 'conditions' in rule:
            conditions = rule['conditions']
            if not isinstance(conditions, list) or len(conditions) == 0:
                self._add_error(f"Rule '{rule_name}': Must have at least one condition")
            elif len(conditions) > self.MAX_CONDITIONS:
                self._add_error(f"Rule '{rule_name}': Too many conditions ({len(conditions)})")
            else:
                for i, cond in enumerate(conditions):
                    self._validate_condition(cond, rule_name, i)
        
        # Validate actions
        if 'actions' in rule:
            actions = rule['actions']
            if not isinstance(actions, list) or len(actions) == 0:
                self._add_error(f"Rule '{rule_name}': Must have at least one action")
            elif len(actions) > self.MAX_ACTIONS:
                self._add_error(f"Rule '{rule_name}': Too many actions ({len(actions)})")
            else:
                for i, action in enumerate(actions):
                    self._validate_action(action, rule_name, i)
                self._check_action_conflicts(actions, rule_name)
    
    def _validate_condition(self, condition: Dict, rule_name: str, idx: int):
        """Validate a single condition"""
        for field in ['field', 'predicate', 'value']:
            if field not in condition:
                self._add_error(f"Rule '{rule_name}', Condition #{idx + 1}: Missing '{field}'")
                return
        
        field = condition['field']
        predicate = condition['predicate']
        
        # Validate field
        if field not in self.SUPPORTED_FIELDS:
            self._add_error(
                f"Rule '{rule_name}', Condition #{idx + 1}: Invalid field '{field}'. "
                f"Supported: from, subject, message, received_date"
            )
            return
        
        field_type = self.SUPPORTED_FIELDS[field]
        
        # Validate predicate for field type
        if field_type == 'string' and predicate not in self.STRING_PREDICATES:
            self._add_error(
                f"Rule '{rule_name}', Condition #{idx + 1}: "
                f"Invalid predicate '{predicate}' for string field"
            )
        elif field_type == 'date':
            if predicate not in self.DATE_PREDICATES:
                self._add_error(
                    f"Rule '{rule_name}', Condition #{idx + 1}: "
                    f"Invalid predicate '{predicate}' for date field"
                )
            
            if 'unit' not in condition:
                self._add_error(
                    f"Rule '{rule_name}', Condition #{idx + 1}: "
                    f"Date conditions require 'unit' (days/months)"
                )
            elif condition['unit'] not in self.VALID_DATE_UNITS:
                self._add_error(
                    f"Rule '{rule_name}', Condition #{idx + 1}: "
                    f"Invalid unit '{condition['unit']}'"
                )
            
            # Validate numeric value
            try:
                val = int(condition['value'])
                if val <= 0:
                    self._add_error(
                        f"Rule '{rule_name}', Condition #{idx + 1}: "
                        f"Date value must be positive"
                    )
            except ValueError:
                self._add_error(
                    f"Rule '{rule_name}', Condition #{idx + 1}: "
                    f"Date value must be numeric"
                )
    
    def _validate_action(self, action: Dict, rule_name: str, idx: int):
        """Validate a single action"""
        if 'type' not in action:
            self._add_error(f"Rule '{rule_name}', Action #{idx + 1}: Missing 'type'")
            return
        
        action_type = action['type']
        
        if action_type not in self.SUPPORTED_ACTIONS:
            self._add_error(
                f"Rule '{rule_name}', Action #{idx + 1}: "
                f"Invalid action '{action_type}'"
            )
            return
        
        if action_type == 'move_message':
            if 'destination' not in action:
                self._add_error(
                    f"Rule '{rule_name}', Action #{idx + 1}: "
                    f"'move_message' requires 'destination'"
                )
            elif not action['destination'] or len(action['destination'].strip()) == 0:
                self._add_error(
                    f"Rule '{rule_name}', Action #{idx + 1}: "
                    f"'destination' cannot be empty"
                )
    
    def _check_action_conflicts(self, actions: List[Dict], rule_name: str):
        """Check for conflicting or redundant actions"""
        action_types = [a.get('type') for a in actions]
        
        # Conflicting read/unread
        if 'mark_as_read' in action_types and 'mark_as_unread' in action_types:
            self._add_error(
                f"Rule '{rule_name}': Cannot have both 'mark_as_read' and 'mark_as_unread'"
            )
        
        # Redundant actions
        if action_types.count('mark_as_read') > 1:
            self._add_warning(f"Rule '{rule_name}': Duplicate 'mark_as_read' actions")
        
        if action_types.count('mark_as_unread') > 1:
            self._add_warning(f"Rule '{rule_name}': Duplicate 'mark_as_unread' actions")
        
        # Multiple move actions
        move_actions = [a for a in actions if a.get('type') == 'move_message']
        if len(move_actions) > 1:
            destinations = [a.get('destination') for a in move_actions]
            if len(set(destinations)) > 1:
                self._add_warning(
                    f"Rule '{rule_name}': Multiple move actions - "
                    f"email will have all labels: {', '.join(destinations)}"
                )
    
    def _add_error(self, message: str):
        self.errors.append(f"ERROR: {message}")
    
    def _add_warning(self, message: str):
        self.warnings.append(f"WARNING: {message}")
    
    def _format_errors(self) -> str:
        messages = []
        if self.errors:
            messages.append("Validation Errors:")
            messages.extend(f"  {e}" for e in self.errors)
        if self.warnings:
            messages.append("\nWarnings:")
            messages.extend(f"  {w}" for w in self.warnings)
        return "\n".join(messages)