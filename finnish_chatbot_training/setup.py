import os
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_project():
    """Initialize project environment"""

    directories = [
        "data/raw",
        "data/processed",
        "data/training",
        "logs"
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensuring directory exists: {directory}")


    logger.info("Starting NLTK data download...")
    try:
        subprocess.run(["python", "download_nltk.py"], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to download NLTK data: {str(e)}")

    logger.info("Project environment setup complete")

if __name__ == "__main__":
    setup_project()