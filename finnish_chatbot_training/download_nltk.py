import nltk
import ssl
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_ssl_context():
    """Setup SSL context to handle certificate issues"""
    try:
        # Disable SSL certificate verification
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            logger.warning("Could not create unverified HTTPS context. SSL verification may fail.")
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context
            logger.info("Set up unverified HTTPS context for downloads")
    except Exception as e:
        logger.error(f"Error setting up SSL context: {e}")

def download_nltk_data():
    """Download required NLTK data with robust error handling"""
    resources = [
        ('punkt', 'Tokenizer for breaking text into words'),
        ('stopwords', 'Common stopwords in multiple languages')
    ]

    success = True

    for resource, description in resources:
        try:
            logger.info(f"Downloading {resource} ({description})...")
            nltk.download(resource, quiet=False)
            logger.info(f"Successfully downloaded {resource}")
        except Exception as e:
            logger.error(f"Failed to download {resource}: {e}")
            success = False

    return success

def verify_nltk_data():
    """Verify that required NLTK data is available"""
    resources_to_check = [
        ('punkt', 'tokenizers/punkt'),
        ('stopwords', 'corpora/stopwords')
    ]

    all_available = True

    logger.info("Verifying NLTK data availability...")

    for name, path in resources_to_check:
        try:
            nltk.data.find(path)
            logger.info(f"✓ {name} is available")
        except LookupError:
            logger.error(f"✗ {name} is NOT available")
            all_available = False

    return all_available

if __name__ == "__main__":
    logger.info("Starting NLTK data download and verification...")


    setup_ssl_context()


    download_success = download_nltk_data()


    verification_success = verify_nltk_data()

    if download_success and verification_success:
        logger.info("All NLTK data successfully downloaded and verified!")
        sys.exit(0)
    else:
        logger.warning("Some NLTK resources could not be downloaded or verified.")
        logger.info("For Finnish datasets, this may not affect functionality if fallbacks are implemented.")
        sys.exit(1)