import os
import pandas as pd
import logging
from data_processor import DataProcessor
from database_manager import DatabaseManager
from conversation_processor import ConversationProcessor


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_process_corrupted():
    """Test handling of a corrupted data file"""
    corrupted_file = "data/raw/corrupted_test.csv"


    if not os.path.exists(corrupted_file):
        logger.error(f"The corrupted test file {corrupted_file} does not exist. Please run test_robustness.py first.")
        return

    logger.info(f"Starting to process the corrupted test file: {corrupted_file}")


    processor = DataProcessor()
    processed_file = processor.process_file(corrupted_file)

    if not processed_file:
        logger.error("Failed to process the corrupted file")
        return

    logger.info(f"Successfully processed the corrupted file. Output saved to: {processed_file}")


    db_manager = DatabaseManager()
    success = db_manager.store_data(processed_file)

    if success:
        logger.info("Successfully stored the processed file in the database")
    else:
        logger.warning("Failed to store the file in the database")


    conversation_processor = ConversationProcessor(output_dir="data/training")

    try:
        stats = conversation_processor.process_csv_to_jsonl(processed_file)
        logger.info(f"Successfully generated training data: {stats['single_turn_count']} single-turn and {stats['multi_turn_count']} multi-turn conversations")
    except Exception as e:
        logger.error(f"Error while generating training data: {str(e)}")


    logger.info("Test completed")

if __name__ == "__main__":
    test_process_corrupted()
