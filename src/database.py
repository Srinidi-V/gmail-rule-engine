"""
Database layer with temporal tracking (bitemporal design)
Tracks email state changes over time
"""
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from typing import List, Dict, Optional
from datetime import datetime
import json


class EmailDatabase:
    def __init__(self, db_config):
        self.db_config = db_config
        self.create_tables()

    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)

    def create_tables(self):
        """Create database tables with temporal tracking"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Create emails table with temporal columns
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS emails (
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
                    
                    PRIMARY KEY (email_id, valid_from),
                    CONSTRAINT valid_period CHECK (valid_to IS NULL OR valid_to > valid_from)
                )
            """
            )

            # Indexes for performance
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_email_current 
                ON emails(email_id) WHERE is_current = TRUE
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_from_email 
                ON emails(from_email) WHERE is_current = TRUE
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_subject 
                ON emails(subject) WHERE is_current = TRUE
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_received_date 
                ON emails(received_date) WHERE is_current = TRUE
            """
            )

            conn.commit()
            print("Database tables created/verified with temporal tracking")

        except Exception as e:
            print(f"Error creating tables: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def insert_or_update_email(self, email_data: Dict):
        """Insert email or create new version if changed"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            email_id = email_data["id"]
            now = datetime.now()

            cursor.execute(
                """
                SELECT * FROM emails 
                WHERE email_id = %s AND is_current = TRUE
            """,
                (email_id,),
            )

            existing = cursor.fetchone()

            if existing:
                has_changed = self._has_email_changed(existing, email_data)

                if has_changed:
                    cursor.execute(
                        """
                        UPDATE emails 
                        SET valid_to = %s, is_current = FALSE
                        WHERE email_id = %s AND is_current = TRUE
                    """,
                        (now, email_id),
                    )

                    self._insert_new_version(cursor, email_data, now)
            else:
                self._insert_new_version(cursor, email_data, now)

            conn.commit()

        except Exception as e:
            print(f"Error inserting email: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def _has_email_changed(self, existing: Dict, new_data: Dict) -> bool:
        """Check if email data has changed (mainly labels)"""
        existing_labels = set(
            json.loads(existing["labels"]) if existing["labels"] else []
        )
        new_labels = set(new_data.get("labels", []))
        return existing_labels != new_labels

    def _insert_new_version(self, cursor, email_data: Dict, valid_from: datetime):
        """Insert a new version of an email"""
        cursor.execute(
            """
            INSERT INTO emails 
            (email_id, valid_from, thread_id, from_email, to_email, 
             subject, message, received_date, labels, valid_to, is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
        """,
            (
                email_data["id"],
                valid_from,
                email_data.get("thread_id", ""),
                email_data.get("from", ""),
                email_data.get("to", ""),
                email_data.get("subject", ""),
                email_data.get("message", ""),
                email_data.get("received_date"),
                json.dumps(email_data.get("labels", [])),
            ),
        )

    def insert_emails_batch(self, emails: List[Dict]):
        """Insert multiple emails with temporal tracking"""
        print(f"\nProcessing {len(emails)} emails...")
        for email in emails:
            self.insert_or_update_email(email)

    def get_all_emails(self) -> List[Dict]:
        """Retrieve all CURRENT emails (latest versions only)"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cursor.execute(
                """
                SELECT * FROM emails 
                WHERE is_current = TRUE 
                ORDER BY received_date DESC
            """
            )
            rows = cursor.fetchall()

            emails = []
            for row in rows:
                emails.append(
                    {
                        "id": row["email_id"],
                        "thread_id": row["thread_id"],
                        "from": row["from_email"],
                        "to": row["to_email"],
                        "subject": row["subject"],
                        "message": row["message"],
                        "received_date": row["received_date"],
                        "labels": json.loads(row["labels"]) if row["labels"] else [],
                    }
                )

            return emails

        except Exception as e:
            print(f"Error fetching emails: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_email_by_id(self, email_id: str) -> Optional[Dict]:
        """
        Retrieve a single current email by ID
        Only used for testing purposes.
        """
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cursor.execute(
                """
                SELECT * FROM emails 
                WHERE email_id = %s AND is_current = TRUE
                AND valid_to IS NULL
                ORDER BY received_date DESC NULLS LAST
                LIMIT 1
            """,
                (email_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row["email_id"],
                "thread_id": row["thread_id"],
                "from": row["from_email"],
                "to": row["to_email"],
                "subject": row["subject"],
                "message": row["message"],
                "received_date": row["received_date"],
                "labels": json.loads(row["labels"]) if row["labels"] else [],
            }
        except Exception as e:
            print(f"Error fetching email by id: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def count_emails(self) -> int:
        """Get count of current active unique emails"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT email_id) 
                FROM emails 
                WHERE is_current = TRUE
            """
            )
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error counting emails: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    def get_stats(self) -> Dict:
        """Get database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cursor.execute(
                """
                SELECT 
                    COUNT(DISTINCT email_id) as unique_emails,
                    COUNT(*) as total_versions,
                    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_versions,
                    SUM(CASE WHEN is_current THEN 0 ELSE 1 END) as historical_versions
                FROM emails
            """
            )

            stats = cursor.fetchone()
            return dict(stats) if stats else {}

        except Exception as e:
            print(f"Error fetching stats: {e}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def get_email_history(self, email_id: str) -> List[Dict]:
        """
        Return all versions for a given email ordered by valid_from ASC.
        Each entry includes: id, labels, is_current, valid_from.
        Used only for testing purposes.
        """
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(
                """
                SELECT email_id, labels, is_current, valid_from
                FROM emails
                WHERE email_id = %s
                ORDER BY valid_from ASC
                """,
                (email_id,),
            )
            rows = cursor.fetchall()
            history: List[Dict] = []
            for row in rows:
                history.append(
                    {
                        "id": row["email_id"],
                        "labels": json.loads(row["labels"]) if row["labels"] else [],
                        "is_current": row["is_current"],
                        "valid_from": row["valid_from"],
                    }
                )
            return history
        except Exception as e:
            print(f"Error fetching email history: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_stored_email_ids(self) -> List[str]:
        """
        Return list of email_ids that currently exist (is_current = TRUE).
        Only used for testing purposes.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DISTINCT email_id
                FROM emails
                WHERE is_current = TRUE
                """
            )
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            print(f"Error fetching stored email ids: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
