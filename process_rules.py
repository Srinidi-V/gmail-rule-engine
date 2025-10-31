"""
Process emails based on rules with automatic validation

Usage:
    python3 process_rules.py                    # Use default rules.json
    python3 process_rules.py custom_rules.json  # Use custom file
    python3 process_rules.py --rules my_rules.json
"""
import sys
import argparse
from src.gmail_client import GmailClient
from src.database import EmailDatabase
from src.rule_engine import RuleEngine
from src.rule_validator import RuleValidationError
from config import DB_CONFIG, DEFAULT_RULES_FILE

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Process emails based on configurable rules (with validation)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Use default rules.json
  %(prog)s custom_rules.json         # Use custom rules file
  %(prog)s --rules my_rules.json     # Use custom rules file
        """
    )
    
    parser.add_argument(
        'rules_file',
        nargs='?',
        default=DEFAULT_RULES_FILE,
        help=f'Path to rules JSON file (default: {DEFAULT_RULES_FILE})'
    )
    
    parser.add_argument(
        '--rules', '-r',
        dest='rules_file_flag',
        help='Path to rules JSON file (alternative syntax)'
    )
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    rules_file = args.rules_file_flag if args.rules_file_flag else args.rules_file
    
    print("=" * 60)
    print("Gmail Rule Processor")
    print("=" * 60)
    print(f"Rules file: {rules_file}")
    print("=" * 60)
    
    # Initialize components
    print("\nStep 1: Initializing components...")
    gmail = GmailClient()
    db = EmailDatabase(DB_CONFIG)
    
    # Load and validate rules (validation integrated in RuleEngine)
    try:
        engine = RuleEngine(rules_file)
    except RuleValidationError:
        print("\nCannot proceed due to validation errors.")
        print("Fix the errors above and try again.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\nError: Rules file '{rules_file}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"\nError loading rules: {e}")
        sys.exit(1)
    
    # Load emails from database
    print("\nStep 2: Loading emails from database...")
    emails = db.get_all_emails()
    
    if not emails:
        print("No emails found in database. Run fetch_emails.py first.")
        return
    
    print(f"Loaded {len(emails)} emails")
    
    # Process each email
    print(f"\nStep 3: Evaluating rules for each email...")
    print("-" * 60)
    
    processed_count = 0
    action_count = 0
    
    for i, email in enumerate(emails, 1):
        print(f"[{i}/{len(emails)}] Processing...", end='\r')
        
        actions = engine.evaluate_rules(email)
        
        if actions:
            processed_count += 1
            for action in actions:
                if execute_action(gmail, email, action):
                    action_count += 1
    
    # Summary
    print("\n" + "=" * 60)
    print(f"COMPLETE:")
    print(f"  - Emails matched: {processed_count}/{len(emails)}")
    print(f"  - Actions executed: {action_count}")
    print("=" * 60)

def execute_action(gmail: GmailClient, email: dict, action: dict) -> bool:
    """Execute an action on an email"""
    action_type = action['type']
    
    try:
        if action_type == 'mark_as_read':
            print(f"Marking as read...")
            return gmail.mark_as_read(email['id'])
        
        elif action_type == 'mark_as_unread':
            print(f"Marking as unread...")
            return gmail.mark_as_unread(email['id'])
        
        elif action_type == 'move_message':
            destination = action.get('destination', 'Processed')
            print(f"Moving to '{destination}'...")
            return gmail.move_message(email['id'], destination)
        
        else:
            print(f"Unknown action type: {action_type}")
            return False
            
    except Exception as e:
        print(f"Error executing action: {e}")
        return False

if __name__ == '__main__':
    main()