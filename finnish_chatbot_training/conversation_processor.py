import pandas as pd
import json
import os
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConversationProcessor:
    def __init__(self, output_dir="data/training"):
        """Initialize conversation processor"""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def process_csv_to_jsonl(self, csv_file):
        """Convert processed CSV to JSONL format for training data"""
        try:
            logger.info(f"Starting to process file: {csv_file}")
            df = pd.read_csv(csv_file, encoding='utf-8')


            base_filename = os.path.basename(csv_file).split('.')[0]


            single_turn_path = os.path.join(self.output_dir, f"{base_filename}_single_turn.jsonl")
            single_turn_count = self._create_single_turn_data(df, single_turn_path)


            multi_turn_path = os.path.join(self.output_dir, f"{base_filename}_multi_turn.jsonl")
            multi_turn_count = self._create_multi_turn_data(df, multi_turn_path)

            logger.info(f"Processing complete. Generated {single_turn_count} single-turn and {multi_turn_count} multi-turn conversations")

            return {
                "single_turn_path": single_turn_path,
                "multi_turn_path": multi_turn_path,
                "single_turn_count": single_turn_count,
                "multi_turn_count": multi_turn_count
            }

        except Exception as e:
            logger.error(f"Error processing CSV to generate training data: {str(e)}")
            raise

    def _create_single_turn_data(self, df, output_path):
        """Create single-turn conversation data"""
        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for _, row in df.iterrows():
                try:
                    if not isinstance(row.get('text', ''), str) or len(row.get('text', '').strip()) < 10:
                        continue

                    conversation = {
                        "messages": [
                            {"role": "human", "content": "Please respond in Finnish style to the following content"},
                            {"role": "assistant", "content": row.get('text', '')}
                        ]
                    }

                    f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
                    count += 1
                except Exception as e:
                    logger.warning(f"Error processing single-turn conversation row: {str(e)}")
                    continue

        logger.info(f"JSONL file saved: {output_path}")
        return count

    def _create_multi_turn_data(self, df, output_path):
        """Create multi-turn conversation data"""
        count = 0


        if 'post_id' not in df.columns:
            with open(output_path, 'w', encoding='utf-8') as f:
                pass
            return 0

        try:

            grouped = df.groupby('post_id')

            with open(output_path, 'w', encoding='utf-8') as f:
                for post_id, group in grouped:
                    if len(group) > 1:
                        messages = []

                        for i, (_, comment) in enumerate(group.iterrows()):
                            if i % 2 == 0:
                                messages.append({"role": "human", "content": comment.get('text', '')})
                            else:
                                messages.append({"role": "assistant", "content": comment.get('text', '')})

                        if len(messages) >= 2:
                            conversation = {"messages": messages}
                            f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
                            count += 1

            logger.info(f"JSONL file saved: {output_path}")
            return count

        except Exception as e:
            logger.error(f"Error generating multi-turn conversation data: {str(e)}")
            with open(output_path, 'w', encoding='utf-8') as f:
                pass
            return 0