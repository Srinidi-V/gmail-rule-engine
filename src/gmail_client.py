"""
Gmail API client for fetching and modifying emails
Handles OAuth authentication and all Gmail API operations
"""
import os.path
import pickle
import base64
from email.utils import parsedate_to_datetime
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import GMAIL_SCOPES, CREDENTIALS_FILE, TOKEN_FILE


class GmailClient:
    def __init__(self):
        self.service = None
        self.authenticate()

    def authenticate(self):
        """Authenticate with Gmail API using OAuth2"""
        creds = None

        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILE, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(TOKEN_FILE, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", "v1", credentials=creds)
        print("Gmail authentication successful")

    def fetch_emails(self, max_results=50):
        """Fetch emails from Gmail inbox"""
        try:
            print(f"Fetching up to {max_results} emails...")
            results = (
                self.service.users()
                .messages()
                .list(userId="me", labelIds=["INBOX"], maxResults=max_results)
                .execute()
            )

            messages = results.get("messages", [])

            if not messages:
                print("No messages found.")
                return []

            emails = []
            for i, msg in enumerate(messages, 1):
                print(f"  Fetching email {i}/{len(messages)}...", end="\r")
                email_data = self.get_email_details(msg["id"])
                if email_data:
                    emails.append(email_data)

            print(f"\nSuccessfully fetched {len(emails)} emails")
            return emails

        except HttpError as error:
            print(f"Error fetching emails: {error}")
            return []

    def get_email_details(self, msg_id):
        """Get detailed information about a specific email"""
        try:
            message = (
                self.service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )

            headers = message["payload"]["headers"]
            header_dict = {h["name"]: h["value"] for h in headers}

            subject = header_dict.get("Subject", "")
            from_email = header_dict.get("From", "")
            to_email = header_dict.get("To", "")
            date_str = header_dict.get("Date", "")

            received_date = None
            if date_str:
                try:
                    received_date = parsedate_to_datetime(date_str)
                except:
                    received_date = None

            body = self._get_email_body(message["payload"])

            return {
                "id": msg_id,
                "thread_id": message["threadId"],
                "from": from_email,
                "to": to_email,
                "subject": subject,
                "message": body,
                "received_date": received_date,
                "labels": message.get("labelIds", []),
            }

        except HttpError as error:
            print(f"Error fetching email {msg_id}: {error}")
            return None

    def _get_email_body(self, payload):
        """Extract email body from payload"""
        body = ""

        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/plain":
                    if "data" in part["body"]:
                        body = base64.urlsafe_b64decode(part["body"]["data"]).decode(
                            "utf-8", errors="ignore"
                        )
                        break
        elif "body" in payload and "data" in payload["body"]:
            body = base64.urlsafe_b64decode(payload["body"]["data"]).decode(
                "utf-8", errors="ignore"
            )

        return body[:1000]  # Limit to first 1000 chars

    def mark_as_read(self, msg_id):
        """Mark email as read"""
        try:
            self.service.users().messages().modify(
                userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
            ).execute()
            return True
        except HttpError as error:
            print(f"Error marking as read: {error}")
            return False

    def mark_as_unread(self, msg_id):
        """Mark email as unread"""
        try:
            self.service.users().messages().modify(
                userId="me", id=msg_id, body={"addLabelIds": ["UNREAD"]}
            ).execute()
            return True
        except HttpError as error:
            print(f"Error marking as unread: {error}")
            return False

    def move_message(self, message_id: str, destination_label: str) -> bool:
        """
        Move message to specified label (matches traditional email folder behavior)
        Removes the message from previous locations and places it in the
        destination label
        """
        try:
            destination_id = self._get_or_create_label(destination_label)

            msg = (
                self.service.users()
                .messages()
                .get(userId="me", id=message_id, format="minimal")
                .execute()
            )

            current_labels = msg.get("labelIds", [])

            all_labels = self.service.users().labels().list(userId="me").execute()
            label_info = {l["id"]: l for l in all_labels.get("labels", [])}

            labels_to_remove = []

            location_labels = ["INBOX", "SENT", "DRAFT", "TRASH", "SPAM"]

            for label_id in current_labels:
                label = label_info.get(label_id, {})

                if label_id in location_labels:
                    labels_to_remove.append(label_id)

                elif label.get("type") == "user" and label_id != destination_id:
                    labels_to_remove.append(label_id)

            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={
                    "addLabelIds": [destination_id],
                    "removeLabelIds": labels_to_remove,
                },
            ).execute()

            return True

        except Exception as e:
            print(f"✗ Error moving message: {e}")
            return False

    def _get_or_create_label(self, label_name):
        """Get existing label or create new one"""
        try:
            results = self.service.users().labels().list(userId="me").execute()
            labels = results.get("labels", [])

            for label in labels:
                if label["name"] == label_name:
                    return label["id"]

            label_object = {
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            }
            created_label = (
                self.service.users()
                .labels()
                .create(userId="me", body=label_object)
                .execute()
            )

            print(f"Created new label: {label_name}")
            return created_label["id"]

        except HttpError as error:
            print(f"Error with label: {error}")
            return None
