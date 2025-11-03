# MySQL Retail Data ETL & Incremental Updater

This repository contains a set of Python scripts for Extract, Transform, Load (ETL) operations and incremental updates for retail-related data into a MySQL database. It processes CSV files representing various retail entities (brands, products, customers, orders, etc.), transforms them into a clean, normalized format, and loads them into a relational database, supporting both initial bulk loads and ongoing daily updates.

## Features

*   **Full ETL Pipeline:** Performs a complete extract, transform, and load of all specified datasets, replacing existing tables.
*   **Incremental Data Updates:** Efficiently updates the database by detecting changes (upserts) and handling missing records (deletes) based on full CSV snapshots.
*   **Data Transformation:** Applies consistent renames, type conversions, string normalization, and data aggregation (e.g., for stocks) across all data.
*   **Database Schema Management:** Creates and manages primary keys, unique constraints, and foreign key relationships using SQLAlchemy and MySQL.
*   **Timestamped Archiving:** Moves processed input CSV files into a timestamped archive folder, preserving a history of source data.
*   **Externalized Configuration:** All project settings, including paths, database connection details, and dataset-specific metadata (keys, dtypes, constraints), are managed via a `config.yaml` file.
*   **Modular Design:** Shared utility functions are consolidated into `utils.py` for consistency and maintainability.
*   **`last_updated` Tracking:** Automatically adds and updates a `last_updated` timestamp (minute-precision UTC) for all loaded records.

<img width="1059" height="613" alt="image" src="https://github.com/user-attachments/assets/ded36037-b46c-4b69-aa6b-2af3ad2f4ae1" />


## Prerequisites

Before running the scripts, ensure you have the following installed:

*   **Python 3.8+**
*   **MySQL Server:** A running MySQL server instance.
*   **`pip`** (Python package installer)

## Setup

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **Install Python Dependencies:**
    It's recommended to use a virtual environment.
    ```bash
    # Create a virtual environment (optional but recommended)
    python -m venv venv
    source venv/bin/activate # On Windows, use `venv\Scripts\activate`

    # Install required packages
    pip install -r requirements.txt
    ```
    The `requirements.txt` file should contain:
    ```
    pandas
    SQLAlchemy
    PyMySQL
    PyYAML
    pymysql
    cryptography
    ```

3.  **MySQL Database Configuration:**
    *   Ensure your MySQL server is running.
    *   Create the target database (e.g., `mydb`) on your MySQL server if it doesn't already exist.

4.  **Database Credentials File:**
    Create a plain text file named `db_credentials.txt` in the root directory of this repository. This file should contain your MySQL username on the first line and your password on the second line:
    ```
    your_mysql_username
    your_mysql_password
    ```
    **Note:** For production environments, consider more secure methods for managing credentials than a plain text file (e.g., environment variables, a secrets management service).

5.  **Data Folder:**
    Create a directory named `data/` in the root of the repository. All your input CSV files (e.g., `brands.csv`, `products.csv`, `staffs.csv`) must be placed directly into this `data/` directory. A dataset is stored `backup` directory, and can be copied to data. The update and error dir contains files that should either update the database or had errors during processing.

## Configuration (`config.yaml`)

The `config.yaml` file centralizes all operational parameters for both the ETL and incremental update scripts.

*   **`paths`**: Defines the `data_root` (where input CSVs are expected) and `archive_root` (where processed CSVs are moved).
*   **`db`**: Contains MySQL connection details and specifies the `credentials_file`.
*   **`sql`**: General SQL settings, such as `varchar_len` for string columns.
*   **`load_sequence`**: An ordered list of dataset names, crucial for respecting foreign key dependencies during loading and updates.
*   **`datasets`**: A dictionary where each key represents a dataset (e.g., `brands`, `products`). Each dataset entry includes:
    *   `file`: The name of the corresponding CSV input file.
    *   `table`: The target table name in the MySQL database.
    *   `key_columns`: Columns used to uniquely identify rows for matching during incremental updates/deletes.
    *   `primary_key`: Columns forming the primary key in the database table.
    *   `unique_constraints`: Other unique constraints (composite keys are supported).
    *   `foreign_keys`: Definitions for foreign key relationships, including `on_delete` and `on_update` actions.
    *   `dtype`: A mapping of column names to their SQLAlchemy types, including length specifications for `String` types.

## Usage

### 1. Initial Full ETL Load

Use the `etl.py` script to perform a complete load of all data, typically when setting up the database for the first time or if a full refresh is needed.

1.  **Place all required CSV files** (e.g., `brands.csv`, `categories.csv`, `products.csv`, etc.) into the `data/` directory.
2.  Run the ETL script:
    ```bash
    python etl.py
    ```
    This script will:
    *   Read all CSVs from `data/`.
    *   Apply transformations and add `last_updated` timestamps.
    *   **Drop and recreate** all tables in the database, then load the data.
    *   Add primary keys, unique constraints, and foreign keys as defined in `config.yaml`.
    *   Move all processed CSVs from `data/` to a timestamped folder within `archive/`.

### 2. Incremental Updates

Use the `update_incremental.py` script for ongoing updates. This script is designed to handle new data, changes to existing data, and deletions.

1.  **Place new or updated CSV files** (e.g., `customers.csv` if customer data has changed, `stocks.csv` for stock level updates) into the `data/` directory. Only CSVs present in `data/` will be processed.
2.  Run the incremental update script:
    ```bash
    python update_incremental.py
    ```
    This script will:
    *   Detect which CSV files are present in the `data/` directory.
    *   Process each detected dataset in the order specified in `load_sequence` (to respect FKs).
    *   Read the CSV, apply transformations, and stage it in a temporary `<table>__stg` table.
    *   **Upsert** (update or insert) rows into the main table based on the `key_columns`.
    *   **Delete** rows from the main table that are no longer present in the incoming CSV snapshot (see Assumptions below).
    *   Move the processed CSVs from `data/` to a timestamped folder within `archive/`.

## Project Structure

├── config.yaml               # Centralized configuration for all scripts \
├── db_credentials.txt        # Database username and password (should be ignored by git) \
├── etl.py                    # Script for initial full data load / full refresh \
├── update_incremental.py     # Script for incremental data updates \
├── utils.py                  # Shared utility functions and common logic (e.g., transforms, DB helpers) \
├── get_csvs_from_api.py      # (Optional) Script to fetch CSVs from an external API \
├── requirements.txt          # Python dependencies \
├── data/                     # Input CSV files are placed here (create manually) \
└── archive/                  # Processed CSVs are moved here (created automatically)

## Key Design Choices / Assumptions

*   **Full Snapshot CSVs for Updates:** The `update_incremental.py` script assumes that *each incoming CSV file represents a complete snapshot* of its corresponding dataset. If a record is missing from an incoming CSV but exists in the database, it will be deleted from the database. If your source system provides partial updates, this logic will need modification.
*   **MySQL Focus:** The database interactions and SQL quoting (`_q` function) are specifically tailored for MySQL.
*   **`last_updated` Column:** Every managed table includes a `last_updated` column (datetime, UTC, minute precision) which is set to the current run timestamp upon insertion or update.
*   **Externalized Configuration:** All critical settings are externalized into `config.yaml` for flexibility and ease of management.
*   **Modularity and Readability:** Common functions and configuration are separated into `utils.py` and `config.yaml` to improve script readability and reduce duplication.
*   **Transactionality:** Each `load_to_sql` call in `etl.py` and each `upsert_from_stage` call in `update_incremental.py` runs within its own database transaction. The entire `load_all` process in `etl.py` is not a single transaction.

## Future Improvements

*   **Advanced Error Handling & Logging:** Implement structured logging (e.g., using Python's `logging` module) with different levels and outputs.
*   **Schema Migration Tool:** Integrate a tool like Alembic to manage database schema changes more robustly, rather than relying on `if_exists="replace"` in `etl.py`.
*   **Soft Deletes:** For historical data preservation, implement soft deletes (marking records as inactive) instead of physically removing them from tables.
*   **Data Validation:** Add more comprehensive data validation steps beyond basic type conversion.
*   **Dockerization:** Containerize the application using Docker for easier deployment and environment consistency.
*   **Performance Optimization:** For very large datasets, explore further `df.to_sql` optimizations (e.g., `method='multi'` or custom chunking) and index tuning.

## License

This project is licensed under the MIT License. See the LICENSE file for details. (If you have a LICENSE file in your repo)
