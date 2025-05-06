import praw
import pandas as pd
import time
import logging
import os
from datetime import datetime


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditCollector:
    def __init__(self, client_id, client_secret, user_agent):
        """Initialize Reddit API connection"""
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        self.subreddits = ['Suomi', 'Finland', 'LearnFinnish']


        os.makedirs("data/raw", exist_ok=True)

    def collect_data(self, limit=100, min_comment_length=10):
        """Collect data from the specified subreddits"""
        all_conversations = []

        for subreddit_name in self.subreddits:
            try:
                logger.info(f"Starting data collection from r/{subreddit_name}")
                subreddit = self.reddit.subreddit(subreddit_name)


                for submission in subreddit.hot(limit=limit):

                    if not self._is_finnish(submission.title + submission.selftext):
                        continue

                    submission.comments.replace_more(limit=0)

                    for comment in submission.comments.list():
                        if len(comment.body) >= min_comment_length and self._is_finnish(comment.body):
                            conversation = {
                                'source': 'reddit',
                                'subreddit': subreddit_name,
                                'post_id': submission.id,
                                'post_title': submission.title,
                                'comment_id': comment.id,
                                'text': comment.body,
                                'created_utc': comment.created_utc,
                                'score': comment.score
                            }
                            all_conversations.append(conversation)

                time.sleep(2)

            except Exception as e:
                logger.error(f"Error collecting data from r/{subreddit_name}: {str(e)}")
                continue

        if all_conversations:
            df = pd.DataFrame(all_conversations)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data/raw/reddit_{timestamp}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Collected {len(df)} conversations and saved to {output_file}")
            return output_file
        else:
            logger.warning("No data collected")
            return None

    def _is_finnish(self, text):
        """Detect if text is in Finnish language"""

        if not isinstance(text, str) or len(text.strip()) < 10:
            return False

        try:

            from langdetect import detect, LangDetectException
            try:
                sample_text = text[:1000] if len(text) > 1000 else text
                detected_lang = detect(sample_text)

                return detected_lang == 'fi'
            except LangDetectException as e:
                logger.debug(f"Language detection failed: {str(e)}, falling back to basic detection")

        except ImportError:
            logger.warning("langdetect library not installed, using basic Finnish detection logic")


        finnish_markers = [
            'ja', 'on', 'ei', 'se', 'että', 'kun', 'minä', 'sinä', 'hän', 'me', 'te', 'he',
            'olen', 'olet', 'kuin', 'mutta', 'jos', 'niin', 'mitä', 'hyvä', 'kiitos',
            'suomi', 'suomen', 'voi', 'ovat', 'ole', 'olla', 'mikä', 'missä', 'kuka',
            'paljon', 'vähän', 'suomessa', 'myös', 'pitää', 'vain', 'siis', 'tai'
        ]
        text_lower = text.lower()
        words = text_lower.split()

        finnish_word_count = sum(1 for word in words if word in finnish_markers)

        if len(words) <= 10:
            return finnish_word_count >= 1
        elif len(words) <= 30:
            return finnish_word_count >= 2
        else:

            return finnish_word_count >= 3 or (finnish_word_count >= 2 and finnish_word_count / len(words) >= 0.05)

    def test_connection(self):
        """Test if Reddit API connection works properly"""
        try:
            logger.info("Testing Reddit API connection...")
            me = self.reddit.user.me()
            if me is None:

                subreddit = self.reddit.subreddit("Suomi")
                for _ in subreddit.hot(limit=1):
                    pass
                logger.info("Reddit API connection test successful (read-only mode)")
            else:
                logger.info(f"Reddit API connection test successful, username: {me.name}")
            return True
        except Exception as e:
            logger.error(f"Reddit API connection test failed: {str(e)}")
            return False

if __name__ == "__main__":
    import os


    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")

    if not client_id or not client_secret:
        logger.error("Environment variables REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET not set")
        exit(1)


    collector = RedditCollector(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="finnish_chatbot_data_collector/1.0 (by /u/CatchSerious4869)"
    )


    if collector.test_connection():

        collector.collect_data(limit=50)