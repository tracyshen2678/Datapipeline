
# Data Pipeline Project

This project is a data pipeline that processes and collects data for further analysis. It consists of various Python scripts that work together to collect, process, and store data from various sources, including Reddit and other input data.

## Project Structure

The project consists of the following main components:

1. **conversation_processor.py**: This script is responsible for processing conversations, extracting useful information, and preparing data for further use.
2. **data_processor.py**: This script handles the cleaning, filtering, and transforming of the data into a usable format.
3. **database_manager.py**: The script manages database operations, including saving processed data into a structured format and retrieving data when needed.
4. **download_nltk.py**: This script downloads the necessary resources for the Natural Language Toolkit (NLTK) library, which is used for text processing and analysis.
5. **main.py**: The main script that ties together all the components, running the pipeline and processing the data.
6. **reddit_collector.py**: This script is responsible for collecting data from Reddit, including posts and comments, and preparing it for analysis.

## Setup

Before running the project, make sure to install the necessary dependencies by running:

```bash
pip install -r requirements.txt
```

Make sure you have the following:

- Python 3.x
- NLTK resources
- Reddit API access (if using the `reddit_collector.py` script)

### Running the Project

1. **Download NLTK Resources**: Run the following script to download NLTK resources.

   ```bash
   python download_nltk.py
   ```

2. **Collect Data from Reddit**: Use `reddit_collector.py` to collect Reddit posts and comments.

   ```bash
   python reddit_collector.py
   ```

3. **Process Data**: Run the `data_processor.py` to clean and preprocess the collected data.

   ```bash
   python data_processor.py
   ```

4. **Store Data in Database**: Use `database_manager.py` to store the processed data.

   ```bash
   python database_manager.py
   ```

5. **Run the Main Pipeline**: Finally, you can run the main script to execute the entire pipeline.

   ```bash
   python main.py
   ```

## Contributing

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- This project uses the NLTK library for natural language processing.
- Reddit API is used for data collection.
