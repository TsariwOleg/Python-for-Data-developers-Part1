# Final Project

This project provides functions to transform and aggregate hockey player data from CSV files using PySpark. 
It includes a main application that initializes Spark, reads configuration files, and delegates transformation tasks to various transformers (DataFrame API, SQL API, and RDD API). The project also includes utility functions for reading and writing data, as well as logging and error handling to ensure robust execution.


## Project Structure

```
final-proj/
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   └── io_utils.py
│   ├── transformer/
│   │   ├── __init__.py
│   │   ├── dataframe_api_transformer.py
│   │   ├── rdd_api_transformer.py
│   │   └── sql_api_transformer.py
├── tests/
│   ├── __init__.py
│   └── test_main.py
├── setup.py
├── setup.cfg
├── requirements.txt
├── README.md
```

## Installation

1. **Install Apache Spark 3.1.2**: Ensure you have Apache Spark 3.1.2 installed.

2. **Install Python 3.9**: Ensure you have Python 3.9 installed.

3. **Install setuptools**: Ensure you have setuptools installed.
    ```bash
   pip install setuptools
   ```

1. Clone the repository:
   ```bash
   git clone https://github.com/TsariwOleg/Python-for-Data-developers-Part1.git
   ```

3. Install the library:
   ```bash
   pip install .
   ```

## Running the Program

To run the program, use the following command:

```bash
spark-submit ./src/main/main.py --config-path config.yaml --api 2
```

### Parameters:

- `--config-path`: Path to the YAML configuration file.
- `--api`: The type of API to use for transformations:
  - `1` for DataFrame API
  - `2` for SQL API
  - `3` for RDD API

### Supported File Formats

The program supports default Spark file formats (CSV, Parquet, etc.) for output data.


## Configuration

The configuration file (`config.yaml`) should contain the paths to the source data files and the output path for the final result. Example:

```yaml
source:
  team_path: "path/to/team.csv"
  scoring_path: "path/to/scoring.csv"
  master_path: "path/to/master.csv"
  awards_players_path: "path/to/awards_players.csv"
product:
  final_df: "path/to/output/final_result.csv"
```

## Code Style
To check the code style using pycodestyle, follow these steps:

```bash
pycodestyle .
```

## Running Tests

To run tests, use the following command:

```bash
pytest
```

### Generating Coverage Report

To generate a coverage report, use:

```bash
pytest --cov=src/main --cov-report=html:coverage_report
```
