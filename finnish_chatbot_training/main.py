import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
import os
import time
import logging
import schedule
from reddit_collector import RedditCollector
from data_processor import DataProcessor
from database_manager import DatabaseManager
from conversation_processor import ConversationProcessor



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/training", exist_ok=True)
os.makedirs("logs", exist_ok=True)

def run_pipeline():
    """Run the complete data pipeline"""
    start_time = time.time()
    logger.info("Starting data pipeline execution")


    collected_files = []


    reddit_collector = RedditCollector(
        client_id=os.environ.get("REDDIT_CLIENT_ID"),
        client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
        user_agent="finnish_chatbot_data_collector v1.0"
    )
    reddit_file = reddit_collector.collect_data(limit=200)
    if reddit_file:
        collected_files.append(reddit_file)
        logger.info(f"Collected Reddit data to: {reddit_file}")


    processor = DataProcessor()
    processed_files = []

    for file in collected_files:
        processed_file = processor.process_file(file)
        if processed_file:
            processed_files.append(processed_file)
            logger.info(f"Processed file: {file} -> {processed_file}")


    db_manager = DatabaseManager()

    for file in processed_files:
        success = db_manager.store_data(file)
        if success:
            logger.info(f"Stored {file} to database")
        else:
            logger.warning(f"Failed to store {file} to database")


    conversation_processor = ConversationProcessor(output_dir="data/training")
    training_files = []

    for file in processed_files:
        try:
            stats = conversation_processor.process_csv_to_jsonl(file)
            training_files.append({
                "source": file,
                "single_turn": stats["single_turn_path"],
                "multi_turn": stats["multi_turn_path"],
                "stats": {
                    "single_turn_count": stats["single_turn_count"],
                    "multi_turn_count": stats["multi_turn_count"]
                }
            })
            logger.info(f"Generated training data for {file}: {stats['single_turn_count']} single-turn and {stats['multi_turn_count']} multi-turn conversations")
        except Exception as e:
            logger.error(f"Error generating training data for {file}: {str(e)}")


    db_stats = db_manager.get_stats()
    logger.info(f"Database statistics: {db_stats}")


    total_single_turn = sum(item["stats"]["single_turn_count"] for item in training_files)
    total_multi_turn = sum(item["stats"]["multi_turn_count"] for item in training_files)
    logger.info(f"Training data statistics: Generated {total_single_turn} single-turn and {total_multi_turn} multi-turn conversations")


    elapsed_time = time.time() - start_time
    logger.info(f"Data pipeline execution completed, time elapsed: {elapsed_time:.2f} seconds")

    return {
        "collected_files": collected_files,
        "processed_files": processed_files,
        "training_files": training_files,
        "db_stats": db_stats,
        "training_stats": {
            "total_single_turn": total_single_turn,
            "total_multi_turn": total_multi_turn
        },
        "elapsed_time": elapsed_time
    }

# Set up scheduled tasks
def schedule_pipeline():
    """Set up a daily scheduled task"""
    schedule.every().day.at("02:00").do(run_pipeline)  # Run at 2:00 AM every day

    logger.info("Data pipeline scheduled task set up, will run daily at 2:00 AM")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":

    if "REDDIT_CLIENT_ID" not in os.environ:
        os.environ["REDDIT_CLIENT_ID"] = "your_reddit_client_id"
    if "REDDIT_CLIENT_SECRET" not in os.environ:
        os.environ["REDDIT_CLIENT_SECRET"] = "your_reddit_client_secret"


    import argparse
    parser = argparse.ArgumentParser(description="Data collection and processing pipeline")
    parser.add_argument("--run-once", action="store_true", help="Run pipeline once, don't set up scheduled task")
    parser.add_argument("--skip-schedule", action="store_true", help="Skip immediate run, only set up scheduled task")
    args = parser.parse_args()

    if not args.skip_schedule:

        logger.info("Executing data pipeline immediately")
        run_pipeline()

    if not args.run_once:

        logger.info("Starting scheduled tasks")
        schedule_pipeline()