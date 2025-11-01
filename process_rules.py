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
        description="Process emails based on configurable rules (with validation)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Use default rules.json
  %(prog)s custom_rules.json         # Use custom rules file
  %(prog)s --rules my_rules.json     # Use custom rules file
        """,
    )

    parser.add_argument(
        "rules_file",
        nargs="?",
        default=DEFAULT_RULES_FILE,
        help=f"Path to rules JSON file (default: {DEFAULT_RULES_FILE})",
    )

    parser.add_argument(
        "--rules",
        "-r",
        dest="rules_file_flag",
        help="Path to rules JSON file (alternative syntax)",
    )

    return parser.parse_args()


def execute_action(gmail: GmailClient, email: dict, action: dict) -> bool:
    """
    Execute an action on an email and update the email dict in place

    Args:
        gmail: Gmail client
        email: Email dict (will be modified to reflect changes)
        action: Action to execute

    Returns:
        True if action succeeded, False otherwise
    """
    action_type = action["type"]

    try:
        if action_type == "mark_as_read":
            print(f"Marking as read...")
            success = gmail.mark_as_read(email["id"])

            if success:
                # Update local email state to reflect change
                if "labels" in email and "UNREAD" in email["labels"]:
                    email["labels"] = [l for l in email["labels"] if l != "UNREAD"]
            return success

        elif action_type == "mark_as_unread":
            print(f"Marking as unread...")
            success = gmail.mark_as_unread(email["id"])

            if success:
                if "labels" in email and "UNREAD" not in email["labels"]:
                    email["labels"].append("UNREAD")
            return success

        elif action_type == "move_message":
            destination = action.get("destination", "Processed")
            if destination.lower() in ["inbox", "trash", "spam", "sent", "draft"]:
                destination = destination.upper()
            print(f"Moving to '{destination}'...")
            success = gmail.move_message(email["id"], destination)

            if success:
                # Keep only system labels, add destination
                if "labels" in email:
                    system_labels = [
                        "UNREAD",
                        "STARRED",
                        "IMPORTANT",
                        "CATEGORY_PERSONAL",
                        "CATEGORY_SOCIAL",
                        "CATEGORY_PROMOTIONS",
                        "CATEGORY_UPDATES",
                        "CATEGORY_FORUMS",
                    ]
                    email["labels"] = [l for l in email["labels"] if l in system_labels]
                    email["labels"].append(destination)
                else:
                    email["labels"] = [destination]
            return success

        else:
            print(f"Unknown action type: {action_type}")
            return False

    except Exception as e:
        print(f"Error executing action: {e}")
        return False


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
    emails_to_update = []  # Track emails with label changes

    for i, email in enumerate(emails, 1):

        # Store original labels to detect changes
        original_labels = email.get("labels", []).copy() if email.get("labels") else []

        actions = engine.evaluate_rules(email)

        if actions:
            processed_count += 1
            for action in actions:
                if execute_action(gmail, email, action):
                    action_count += 1

            # Check if labels actually changed
            current_labels = email.get("labels", [])
            if sorted(original_labels) != sorted(current_labels):
                # Labels changed - need to update database
                emails_to_update.append(email)
                print(f"Labels: {original_labels} → {current_labels}")

    print("\n" + "-" * 60)

    # Update database with changed emails
    if emails_to_update:
        print(
            f"\nStep 4: Updating database with {len(emails_to_update)} changed emails..."
        )

        for email in emails_to_update:
            db.insert_or_update_email(email)

        print(f"Database updated with new versions")
    else:
        print(f"\nStep 4: No emails were modified - database unchanged")

    # Summary
    print("\n" + "=" * 60)
    print(f"COMPLETE:")
    print(f"Emails processed: {processed_count}/{len(emails)}")
    print(f"Actions executed: {action_count}")
    print(f"Database versions created: {len(emails_to_update)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
