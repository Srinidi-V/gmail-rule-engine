# Gmail Rule-Based Email Processor

A Python application that automates Gmail email management by fetching emails via Gmail API, storing them in PostgreSQL with temporal tracking, and processing them based on configurable JSON rules.

## 🎯 Features

- **Gmail API Integration**: OAuth2 authentication (no IMAP)
- **PostgreSQL Database**: Temporal tracking of email state changes
- **Rule Engine**: Configurable rules with multiple conditions and predicates
- **Automatic Validation**: Rules validated before execution
- **String Predicates**: Contains, Does not contain, Equals, Does not equal
- **Date Predicates**: Less than / Greater than for days/months
- **Flexible Logic**: Support for ALL (AND) and ANY (OR) condition matching
- **Actions**: Mark as read/unread, Move messages to labels/folders
- **State History**: Track email changes over time

## 📋 Requirements

- Python 3.8+
- PostgreSQL 14+
- Gmail account
- Google Cloud Project with Gmail API enabled

## 🚀 Installation

### 1. Clone Repository
```bash
git clone https://github.com/YOUR_USERNAME/gmail-rule-engine.git
cd gmail-rule-engine
```

### 2. Set Up Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 4. Install PostgreSQL (Mac)
```bash
brew install postgresql@14
brew services start postgresql@14
```

### 5. Create Databases
```bash
createdb gmail_rules
createdb gmail_rules_test
```

### 6. Set Up Gmail API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable Gmail API
4. Create OAuth 2.0 credentials (**Desktop app** type)
5. Download credentials and save as `credentials.json` in project root

## 📖 Usage

### Fetch Emails from Gmail
```bash
# Fetch 50 emails (default)
python3 fetch_emails.py

# Fetch specific number
python3 fetch_emails.py 100
```

### Process Emails with Rules
```bash
# Use default rules.json
python3 process_rules.py

# Use custom rules file
python3 process_rules.py rules_work.json
python3 process_rules.py --rules my_rules.json


## ⚙️ Rule Configuration

### Rule Format (Based on Assignment Requirements)
```json
{
  "rules": [
    {
      "name": "Rule description",
      "predicate": "all",
      "conditions": [
        {
          "field": "from",
          "predicate": "contains",
          "value": "example.com"
        }
      ],
      "actions": [
        {
          "type": "mark_as_read"
        }
      ]
    }
  ]
}
```

### Supported Fields

- `from` - Sender email address
- `subject` - Email subject line
- `message` - Email body content
- `received_date` - Date/time email was received

### String Predicates

- `contains` - Field contains the value
- `does_not_contain` - Field doesn't contain the value
- `equals` - Field exactly matches the value
- `does_not_equal` - Field doesn't match the value

### Date Predicates

- `less_than` - Email older than X days/months
- `greater_than` - Email newer than X days/months

**Requires `unit` field:** `days` or `months`

### Rule Predicates

- `all` - All conditions must match (AND logic)
- `any` - At least one condition must match (OR logic)

### Available Actions

- `mark_as_read` - Mark email as read
- `mark_as_unread` - Mark email as unread
- `move_message` - Move email to specified label/folder (requires `destination`)

## 🗄️ Database Schema (Temporal Tracking)
```sql
CREATE TABLE emails (
    email_id VARCHAR(255) NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    
    thread_id VARCHAR(255),
    from_email TEXT,
    to_email TEXT,
    subject TEXT,
    message TEXT,
    received_date TIMESTAMP,
    labels TEXT,
    
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (email_id, valid_from)
);
```

**Temporal Tracking Benefits:**
- Audit trail of all email state changes
- Time-travel queries (what was the state at 2 PM?)
- No data loss on updates
- Current state queries remain simple

## ✅ Rule Validation

Rules are automatically validated before execution. Validation checks:

- **Structure**: Valid JSON, required fields present
- **Fields**: Only supported fields (from, subject, message, received_date)
- **Predicates**: Correct predicates for field types
- **Values**: Numeric for dates, non-empty where required
- **Actions**: Valid action types, required parameters
- **Conflicts**: Catches conflicting actions (read/unread)

### Validation Errors Stop Execution
```bash
python3 process_rules.py

# If validation fails:
# ✗ Rule validation failed:
# ERROR: Rule 'Test': Invalid field 'sender' (should be 'from')
# ERROR: Rule 'Test': Date conditions require 'unit' field
# 
# Fix the errors above and try again.
```

## 🧪 Testing
```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_rule_engine.py -v
pytest tests/test_validation.py -v
```