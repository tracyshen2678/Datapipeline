import pandas as pd
import re
import os
import logging
import nltk
from nltk.corpus import stopwords


import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    print(f"Automatic NLTK data download failed: {str(e)}")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        """Initialize data processor"""
        # Finnish stopwords list
        self.stopwords = self._load_stopwords()
        logger.info(f"Loaded {len(self.stopwords)} Finnish stopwords")

    def _load_stopwords(self):
        """Load Finnish stopwords"""
        # Custom Finnish stopwords list
        custom_finnish_stopwords = [
            'ja', 'on', 'ei', 'se', 'että', 'kun', 'minä', 'sinä', 'hän', 'me', 'te', 'he',
            'olen', 'olet', 'kuin', 'mutta', 'jos', 'niin', 'mitä', 'hyvä', 'kiitos',
            'suomi', 'suomen', 'voi', 'ovat', 'ole', 'olla', 'mikä', 'missä', 'kuka'
        ]

        try:

            if 'finnish' in stopwords._fileids:
                nltk_stopwords = set(stopwords.words('finnish'))
                logger.info("Using NLTK Finnish stopwords list")
                return nltk_stopwords
        except Exception as e:
            logger.warning(f"Failed to load NLTK stopwords: {str(e)}")


        logger.info("Using custom Finnish stopwords list")
        return set(custom_finnish_stopwords)

    def process_file(self, input_file, max_retries=3):
        """Process a single file with robust error handling"""
        attempt = 0
        errors_detected = 0
        repairs_made = 0

        logger.info(f"CHECKING FILE INTEGRITY: {input_file}")

        while attempt < max_retries:
            try:
                attempt += 1
                if attempt > 1:
                    logger.info(f"RETRY ATTEMPT {attempt}/{max_retries} for file: {input_file}")
                else:
                    logger.info(f"Starting to process file: {input_file}")


                try:
                    df = pd.read_csv(input_file, encoding='utf-8')
                    logger.info(f"File successfully loaded with UTF-8 encoding")
                except pd.errors.ParserError:
                    logger.warning(f"CORRUPTION DETECTED: CSV parser error, attempting repair by skipping bad lines")
                    errors_detected += 1
                    try:
                        df = pd.read_csv(input_file, on_bad_lines='skip')
                        repairs_made += 1
                        logger.info(f"REPAIRED: Successfully loaded file by skipping malformed lines")
                    except TypeError:

                        df = pd.read_csv(input_file, error_bad_lines=False, warn_bad_lines=True)
                        repairs_made += 1
                        logger.info(f"REPAIRED: Successfully loaded file by ignoring bad lines")
                except UnicodeDecodeError:
                    logger.warning(f"CORRUPTION DETECTED: Unicode decode error, attempting repair with alternative encoding")
                    errors_detected += 1
                    df = pd.read_csv(input_file, encoding='latin-1')
                    repairs_made += 1
                    logger.info(f"REPAIRED: Successfully loaded file using latin-1 encoding")


                logger.info(f"VALIDATING data structure and content...")

                missing_columns = []
                for col in ['text', 'source', 'post_id']:
                    if col not in df.columns:
                        missing_columns.append(col)
                        errors_detected += 1

                if missing_columns:
                    logger.warning(f"CORRUPTION DETECTED: Missing required columns: {', '.join(missing_columns)}")
                    for col in missing_columns:
                        df[col] = "" if col == 'text' else "unknown"
                        repairs_made += 1
                    logger.info(f"REPAIRED: Added missing columns with default values")

                null_counts = df.isnull().sum()
                total_nulls = null_counts.sum()

                if total_nulls > 0:
                    logger.warning(f"CORRUPTION DETECTED: Found {total_nulls} null values across {sum(null_counts > 0)} columns")
                    errors_detected += 1


                    if 'text' in df.columns:
                        null_text_count = df['text'].isnull().sum()
                        if null_text_count > 0:
                            logger.warning(f"CORRUPTION DETECTED: {null_text_count} null values in 'text' column")
                            df['text'] = df['text'].fillna("")
                            repairs_made += 1
                            logger.info(f"REPAIRED: Replaced null values in 'text' column with empty strings")


                    try:
                        df['text'] = df['text'].astype(str)
                        logger.info(f"REPAIRED: Converted all text values to string type")
                    except Exception as e:
                        logger.error(f"Failed to convert text column to string: {str(e)}")


                original_count = len(df)
                df.drop_duplicates(subset=['text'], keep='first', inplace=True)
                duplicate_count = original_count - len(df)
                if duplicate_count > 0:
                    logger.info(f"DETECTED: {duplicate_count} duplicate records")
                    logger.info(f"CLEANED: Removed {duplicate_count} duplicate records")


                df = self._filter_content(df)


                logger.info(f"Starting text preprocessing with enhanced error handling...")
                preprocessing_errors = 0

                def preprocess_with_logging(row_text):
                    nonlocal preprocessing_errors
                    try:
                        return self._preprocess_text(row_text)
                    except Exception as e:
                        preprocessing_errors += 1
                        if preprocessing_errors <= 3:
                            logger.warning(f"CORRUPTION DETECTED: Error preprocessing text: {str(e)}. Using fallback.")
                        elif preprocessing_errors == 4:
                            logger.warning(f"More preprocessing errors found but suppressing logs...")

                        if not isinstance(row_text, str):
                            return ""
                        text = str(row_text).lower()

                        text = re.sub(r'[^a-zåäöA-ZÅÄÖ\s]', '', text)
                        return text

                df['processed_text'] = df['text'].apply(preprocess_with_logging)

                if preprocessing_errors > 0:
                    errors_detected += 1
                    repairs_made += 1
                    logger.info(f"REPAIRED: Fixed {preprocessing_errors} text preprocessing errors using fallback method")


                output_file = input_file.replace('/raw/', '/processed/')
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                df.to_csv(output_file, index=False, encoding='utf-8')


                if errors_detected > 0:
                    logger.info(f"SUCCESS: Corrupted file processed despite {errors_detected} issues. Made {repairs_made} repairs.")
                else:
                    logger.info(f"Processing complete, no corruption detected. Saved to {output_file}")

                return output_file

            except Exception as e:
                logger.error(f"ERROR: Processing failure (attempt {attempt}/{max_retries}): {str(e)}")
                if attempt < max_retries:
                    logger.info(f"RETRYING... (Attempt {attempt+1}/{max_retries})")
                else:
                    logger.error(f"FAILED: Could not process file after {max_retries} attempts")
                    return None

    def _filter_content(self, df):
        """Filter inappropriate content"""
        try:
            offensive_words = ['vittu', 'perkele', 'saatana']

            original_count = len(df)

            mask = df['text'].apply(lambda x:
                isinstance(x, str) and any(word in x.lower() for word in offensive_words)
            )
            filtered_df = df[~mask]
            logger.info(f"Filtered out {original_count - len(filtered_df)} records containing inappropriate content")

            return filtered_df
        except Exception as e:
            logger.warning(f"ERROR in content filtering: {str(e)}. Returning original dataframe.")
            return df

    def _preprocess_text(self, text):
        """Preprocess text"""
        try:

            if not isinstance(text, str):
                return ""

            text = text.lower()

            text = re.sub(r'https?://\S+|www\.\S+', '', text)

            text = re.sub(r'<.*?>', '', text)

            text = re.sub(r'[^\w\s]', '', text)
            text = re.sub(r'\d+', '', text)

            tokens = text.split()

            tokens = [word for word in tokens if word not in self.stopwords]

            processed_text = ' '.join(tokens)

            return processed_text

        except Exception as e:
            logger.error(f"Error preprocessing text: {str(e)}")
            return ""

    def _safe_preprocess_text(self, text):
        """Preprocess text with error handling"""
        try:
            return self._preprocess_text(text)
        except Exception as e:
            logger.warning(f"Error preprocessing text: {str(e)}. Using fallback.")

            if not isinstance(text, str):
                return ""
            text = str(text).lower()

            text = re.sub(r'[^a-zåäöA-ZÅÄÖ\s]', '', text)
            return text

if __name__ == "__main__":
    processor = DataProcessor()

    import sys
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        processor.process_file(input_file)
    else:
        logger.info("Usage: python data_processor.py <input_file>")