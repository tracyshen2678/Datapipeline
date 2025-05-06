import os
import pandas as pd
import random


def create_corrupted_data():
    try:
        raw_dir = "data/raw"
        files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
        if not files:
            print("No CSV file found to corrupt")
            return None

        files.sort(key=lambda x: os.path.getctime(os.path.join(raw_dir, x)), reverse=True)
        source_file = os.path.join(raw_dir, files[0])


        df = pd.read_csv(source_file)
        corrupt_file = os.path.join(raw_dir, "corrupted_test.csv")


        drop_indices = random.sample(range(len(df)), int(len(df) * 0.1))
        df = df.drop(drop_indices)

        english_samples = [
            "This is clearly English text and should be filtered out.",
            "Another English sentence for testing purposes.",
            "Random text that is not in Finnish language at all."
        ]

        for _ in range(50):
            sample_row = df.iloc[random.randint(0, len(df)-1)].copy()
            sample_row['text'] = random.choice(english_samples)
            df = pd.concat([df, pd.DataFrame([sample_row])], ignore_index=True)

        null_indices = random.sample(range(len(df)), int(len(df) * 0.05))
        for idx in null_indices:
            df.loc[idx, 'text'] = None

        duplicate_indices = random.sample(range(len(df)), int(len(df) * 0.15))
        duplicates = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)


        if 'subreddit' in df.columns:
            missing_col_indices = random.sample(range(len(df)), int(len(df) * 0.03))
            for idx in missing_col_indices:
                df.loc[idx, 'subreddit'] = None

        df.to_csv(corrupt_file, index=False)
        print(f"Corrupted test file created: {corrupt_file}")
        return corrupt_file

    except Exception as e:
        print(f"Error while creating corrupted data: {e}")
        return None


def create_invalid_credentials_test():

    reddit_client_id = os.environ.get("REDDIT_CLIENT_ID", "")
    reddit_client_secret = os.environ.get("REDDIT_CLIENT_SECRET", "")


    os.environ["REDDIT_CLIENT_ID"] = "invalid_id_for_testing"
    os.environ["REDDIT_CLIENT_SECRET"] = "invalid_secret_for_testing"

    print("Invalid Reddit API credentials set for testing")


    print("To restore correct credentials, use:")
    print(f'export REDDIT_CLIENT_ID="{reddit_client_id}"')
    print(f'export REDDIT_CLIENT_SECRET="{reddit_client_secret}"')

if __name__ == "__main__":
    print("Testing pipeline robustness")
    print("===============")
    print("Choose a test scenario:")
    print("1. Create corrupted data file")
    print("2. Simulate API authentication failure")

    choice = input("Enter your choice (1-2): ")

    if choice == "1":
        create_corrupted_data()
    elif choice == "2":
        create_invalid_credentials_test()
    else:
        print("Invalid choice")
