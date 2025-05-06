import sqlite3
import pandas as pd
import logging
import os
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path="data/finnish_chatbot.db"):
        """Initialize database manager"""
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self._create_tables_if_not_exist()

    def _create_tables_if_not_exist(self):
        """Create necessary tables (if they don't exist)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    text TEXT NOT NULL,
                    processed_text TEXT DEFAULT '',  -- Changed NOT NULL to DEFAULT ''
                    created_at TIMESTAMP,
                    metadata TEXT,
                    created_utc REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')


            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_files (
                    file_path TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.commit()
            logger.info("Database tables created or already exist")

        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
        finally:
            if conn:
                conn.close()

    def store_data(self, processed_file):
        """Store processed data to database"""
        try:

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT file_path FROM processed_files WHERE file_path = ?", (processed_file,))
            if cursor.fetchone():
                logger.info(f"File {processed_file} has already been processed, skipping")
                return False

            df = pd.read_csv(processed_file, encoding='utf-8')

            if 'processed_text' in df.columns:
                df['processed_text'] = df['processed_text'].fillna('')
            else:
                df['processed_text'] = ''

            if 'text' in df.columns:
                df['text'] = df['text'].fillna('')
            else:
                df['text'] = ''

            if 'source' in df.columns:
                df['source'] = df['source'].fillna('unknown')
            else:
                df['source'] = 'unknown'

            successfully_inserted = 0

            for _, row in df.iterrows():
                try:
                    metadata = {}
                    for meta_field in ['subreddit', 'post_id', 'tweet_id', 'score']:
                        if meta_field in row and not pd.isna(row[meta_field]):
                            metadata[meta_field] = row[meta_field]

                    metadata_json = json.dumps(metadata)

                    cursor.execute('''
                        INSERT INTO conversations (source, text, processed_text, created_at, metadata, created_utc)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        row.get('source', 'unknown'),
                        row.get('text', ''),
                        row.get('processed_text', ''),
                        row.get('created_at', None),
                        metadata_json,
                        row.get('created_utc', None)
                    ))
                    successfully_inserted += 1
                except Exception as e:
                    logger.error(f"Error inserting row data: {str(e)}")
                    continue


            cursor.execute("INSERT INTO processed_files (file_path) VALUES (?)", (processed_file,))

            conn.commit()
            logger.info(f"Successfully stored {successfully_inserted} records from {processed_file} to database")
            return True

        except Exception as e:
            logger.error(f"Error storing data to database: {str(e)}")
            if 'conn' in locals() and conn:
                conn.rollback()
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def get_stats(self):
        """Get database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM conversations")
            total_records = cursor.fetchone()[0]

            cursor.execute("SELECT source, COUNT(*) FROM conversations GROUP BY source")
            source_distribution = dict(cursor.fetchall())

            return {
                "total_records": total_records,
                "source_distribution": source_distribution
            }

        except Exception as e:
            logger.error(f"Error getting database statistics: {str(e)}")
            return {}
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    db_manager = DatabaseManager()
    db_manager.store_data("data/processed/reddit_20250406_235231.csv")
    print(db_manager.get_stats())