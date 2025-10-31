"""
Fetch emails from Gmail and store in PostgreSQL database

Usage:
    python3 fetch_emails.py              # Fetch 50 emails (default)
    python3 fetch_emails.py 100          # Fetch 100 emails
    python3 fetch_emails.py --max 200    # Fetch 200 emails
"""
import sys
import argparse
from src.gmail_client import GmailClient
from src.database import EmailDatabase
from config import DB_CONFIG, MAX_EMAILS_TO_FETCH

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Fetch emails from Gmail and store in PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'max_emails',
        nargs='?',
        type=int,
        default=MAX_EMAILS_TO_FETCH,
        help=f'Maximum emails to fetch (default: {MAX_EMAILS_TO_FETCH})'
    )
    
    parser.add_argument(
        '--max', '-m',
        dest='max_emails_flag',
        type=int,
        help='Maximum emails to fetch (alternative syntax)'
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Skip confirmation prompts'
    )
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    max_emails = args.max_emails_flag if args.max_emails_flag else args.max_emails
    
    print("=" * 60)
    print("Gmail Email Fetcher")
    print("=" * 60)
    print(f"Max emails to fetch: {max_emails}")
    print("=" * 60)
    
    print("\nStep 1: Authenticating with Gmail...")
    gmail = GmailClient()
    
    print("\nStep 2: Connecting to database...")
    db = EmailDatabase(DB_CONFIG)
    
    existing_count = db.count_emails()
    print(f"\nStep 3: Database status:")
    print(f"  - Existing emails: {existing_count}")
    
    if existing_count >= max_emails and not args.force:
        print(f"\nDatabase already has {existing_count} emails.")
        response = input("\n    Fetch anyway? [y/N]: ").strip().lower()
        if response != 'y':
            print("\nSkipping fetch. Use --force to override.")
            return
    
    print(f"\nStep 4: Fetching emails from Gmail...")
    emails = gmail.fetch_emails(max_results=max_emails)
    
    if not emails:
        print("No emails fetched. Exiting.")
        return
    
    print(f"\nStoring emails in database...")
    db.insert_emails_batch(emails)
    
    stats = db.get_stats()
    
    print("\n" + "=" * 60)
    print(f"COMPLETE:")
    print(f"  - Fetched: {len(emails)} emails")
    print(f"  - Total unique emails: {stats.get('unique_emails', 0)}")
    print(f"  - Total versions: {stats.get('total_versions', 0)}")
    print(f"  - Historical versions: {stats.get('historical_versions', 0)}")
    print("=" * 60)

if __name__ == '__main__':
    main()