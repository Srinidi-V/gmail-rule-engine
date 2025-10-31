import os

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'gmail_rules'),
    'user': os.getenv('DB_USER', os.getenv('USER')),  # Your Mac username
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432))
}

# Gmail API settings
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'

# Processing settings

# Setting the maximum number of emails to fetch to an optimal value
MAX_EMAILS_TO_FETCH = 50
DEFAULT_RULES_FILE = 'rules.json'