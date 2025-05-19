# Atlan S3 Asset Creator - atlan_create_s3_assets.py

This Python script automates the process of creating AWS S3 assets in Atlan, including connections, buckets, objects, and tables with columns for CSV files. It extracts metadata from an S3 bucket, analyzes CSV files to infer schemas, and creates corresponding assets in your Atlan data catalog.

## Prerequisites

### Python Version

This script has been tested with:

```bash
% python3 --version
Python 3.10.0

```

### Required Python Packages

```bash
pip install boto3 pandas pyatlan==6.0.6

```

### Atlan SDK Version

The script is compatible with:

```bash
% pip3 show pyatlan | grep Version
Version: 6.0.6
Location: /Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages

```

### Required Environment Variables

Set the following environment variables before running the script:

```bash
# Atlan credentials
export ATLAN_API_KEY="your-atlan-api-key"
export ATLAN_BASE_URL="https://your-atlan-instance.com"

# AWS credentials
export AWS_ACCESS_KEY="your-aws-access-key"
export AWS_SECRET_KEY="your-aws-secret-key"

```

Make sure to set these environment variables in your terminal session before running the script. Without these variables, the script will fail to authenticate with both Atlan and AWS.

To verify your environment variables are set correctly:

```bash
echo $ATLAN_API_KEY
echo $ATLAN_BASE_URL
echo $AWS_ACCESS_KEY
echo $AWS_SECRET_KEY

```

### AWS S3 Access

-   Ensure your AWS credentials have read access to the target S3 bucket
-   Verify that the AWS region in the configuration matches your bucket's region

### Atlan Permissions

-   The API key must have permissions to create and modify assets
-   Administrator privileges are recommended to create connections

## Script Overview

### Configuration

The script uses a configuration dictionary that can be customized:

```python
CONFIG = {
    "atlan": {
        "api_key": os.getenv("ATLAN_API_KEY"),  # Gets value from environment variable
        "base_url": os.getenv("ATLAN_BASE_URL")  # Gets value from environment variable
    },
    "aws": {
        "region": "us-east-2",  # Modify to match your bucket's region
        "bucket_name": "atlan-tech-challenge",  # Your S3 bucket name
        "access_key": os.getenv("AWS_ACCESS_KEY"),  # Gets value from environment variable
        "secret_key": os.getenv("AWS_SECRET_KEY")  # Gets value from environment variable
    },
    "unique_identifier": "vvs",  # Change this to your own identifier
    "connection_name": "aws-s3-connection-vv"  # Your desired Atlan connection name
}

```

### Main Features

1.  **S3 Bucket Metadata Extraction**
    
    -   Retrieves bucket location, tags, and other metadata
2.  **S3 Object Discovery and Analysis**
    
    -   Lists all objects in the specified bucket
    -   Extracts metadata like size, type, and last modified date
    -   Detects file format based on extension
3.  **CSV Schema Analysis**
    
    -   For CSV files, detects delimiters automatically
    -   Infers column data types using pandas
    -   Captures sample values for verification
4.  **Atlan Asset Creation**
    
    -   Creates or reuses an S3 connection in Atlan
    -   Creates S3 bucket assets with proper qualified names
    -   Creates S3 object assets for each file in the bucket
    -   For CSV files, creates corresponding table and column assets
5.  **Intelligent Data Type Mapping**
    
    -   Maps pandas types to SQL/Atlan data types:
        -   Integer → INTEGER
        -   Float → DOUBLE
        -   Datetime → TIMESTAMP
        -   Boolean → BOOLEAN
        -   Other → VARCHAR

## Usage

### Setting Environment Variables

Before running the script, set the required environment variables:

**For macOS/Linux:**

```bash
export ATLAN_API_KEY="your-atlan-api-key"
export ATLAN_BASE_URL="https://your-atlan-instance.com"
export AWS_ACCESS_KEY="your-aws-access-key"
export AWS_SECRET_KEY="your-aws-secret-key"

```

**For Windows (Command Prompt):**

```cmd
set ATLAN_API_KEY=your-atlan-api-key
set ATLAN_BASE_URL=https://your-atlan-instance.com
set AWS_ACCESS_KEY=your-aws-access-key
set AWS_SECRET_KEY=your-aws-secret-key

```

**For Windows (PowerShell):**

```powershell
$env:ATLAN_API_KEY="your-atlan-api-key"
$env:ATLAN_BASE_URL="https://your-atlan-instance.com"
$env:AWS_ACCESS_KEY="your-aws-access-key"
$env:AWS_SECRET_KEY="your-aws-secret-key"

```

### Basic Execution

```bash
python3 atlan_create_s3_assets.py

```

### Customization Options

Modify the `CONFIG` dictionary in the script to:

-   Change target AWS region
-   Specify a different S3 bucket
-   Use a custom connection name
-   Change the unique identifier suffix

### Processing Flow

1.  The script connects to both Atlan and AWS S3
2.  Extracts bucket metadata and lists all objects
3.  Analyzes each object, extracting metadata and schema information for CSVs
4.  Creates/reuses an S3 connection in Atlan
5.  Creates a bucket asset in Atlan
6.  Creates object assets for each file
7.  For CSV files, creates table assets with columns based on the inferred schema

### Key Functions

-   `initialize_atlan_client()`: Sets up the connection to Atlan
-   `initialize_s3_client()`: Sets up the connection to AWS S3
-   `extract_s3_bucket_metadata()`: Gets bucket details from AWS
-   `extract_s3_objects_metadata()`: Lists and analyzes objects in the bucket
-   `extract_csv_schema()`: Analyzes CSV files to extract schema information
-   `get_create_s3_connection()`: Creates or gets existing S3 connection in Atlan
-   `get_create_s3_bucket_asset()`: Creates or gets existing bucket asset in Atlan
-   `create_s3_object_assets()`: Creates object assets in Atlan
-   `create_table_from_csv_schema()`: Creates table and column assets from CSV schemas

## Notes

-   The script handles "Unnamed" columns in CSVs by filtering them out
-   It automatically detects CSV delimiters (comma, tab, semicolon, pipe)
-   Objects with existing assets in Atlan will be skipped (idempotent operation)
-   Tables created include descriptions with schema information and delimiter details
-   For large CSV files, only a sample is analyzed to avoid memory issues

## Troubleshooting

-   If you see AWS credential errors, verify your environment variables are set correctly
-   If Atlan connection fails, check your API key and base URL
-   For permission errors in Atlan, ensure your API key has sufficient privileges
-   If CSV analysis fails, check the file format and encoding
-   If you get `KeyError` for environment variables, ensure they are correctly exported in your current shell session





----------------------------------------------------------------------------------------------------------------



# Atlan Table and Column Lineage Creator - atlan_table_column_lineage_creator.py

This Python script automates the creation of end-to-end lineage relationships between tables and columns across PostgreSQL, S3, and Snowflake connections in Atlan. It uses intelligent name matching to identify related tables and columns, then creates appropriate lineage processes to connect them.

## Prerequisites

### Python Version

This script has been tested with:

bash

```bash
% python3 --version
Python 3.10.0
```

### Required Python Packages

bash

```bash
pip install pyatlan==6.0.6 pandas re
```

### Atlan SDK Version

The script is compatible with:

bash

```bash
% pip3 show pyatlan | grep Version
Version: 6.0.6
Location: /Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages
```

### Required Environment Variables

Set the following environment variables before running the script:

bash

```bash
# Atlan credentials
export ATLAN_API_KEY="your-atlan-api-key"
export ATLAN_BASE_URL="https://your-atlan-instance.com"
```

Make sure to set these environment variables in your terminal session before running the script. Without these variables, the script will fail to authenticate with Atlan.

To verify your environment variables are set correctly:

bash

```bash
echo $ATLAN_API_KEY
echo $ATLAN_BASE_URL
```

### Atlan Permissions

-   The API key must have permissions to create and modify assets and lineage relationships
-   Administrator privileges are recommended for full lineage creation capabilities

## Script Overview

### Configuration

The script uses several configuration variables that can be customized:

python

```python
# Connection names for each data platform
POSTGRES_CONNECTION_NAME = "postgres-vv"  # Replace with your PostgreSQL connection name
S3_CONNECTION_NAME = "aws-s3-connection-vv"  # Replace with your S3 connection name
SNOWFLAKE_CONNECTION_NAME = "snowflake-vv"  # Replace with your Snowflake connection name

# Name normalization settings
NORMALIZE_NAMES = True

# Matching thresholds
TABLE_MATCH_THRESHOLD = 80
COLUMN_MATCH_THRESHOLD = 80
```

### Main Features

1.  **Connection and Asset Discovery**
    -   Retrieves connection details for PostgreSQL, S3, and Snowflake
    -   Discovers tables and columns in each connection
2.  **Intelligent Name Matching**
    -   Normalizes names to remove special characters and standardize formats
    -   Calculates similarity scores using Levenshtein distance
    -   Applies configurable threshold values for matching
    -   Handles different naming conventions across systems
3.  **Lineage Creation**
    -   Creates table-level lineage between matched tables
    -   Creates column-level lineage between matched columns
    -   Establishes end-to-end lineage paths (PostgreSQL → S3 → Snowflake)
4.  **Process Documentation**
    -   Creates named processes for each lineage relationship
    -   Adds descriptive information about the data flow
    -   Uses consistent naming conventions for processes

## Usage

### Setting Environment Variables

Before running the script, set the required environment variables:

**For macOS/Linux:**

bash

```bash
export ATLAN_API_KEY="your-atlan-api-key"
export ATLAN_BASE_URL="https://your-atlan-instance.com"
```

**For Windows (Command Prompt):**

cmd

```cmd
set ATLAN_API_KEY=your-atlan-api-key
set ATLAN_BASE_URL=https://your-atlan-instance.com
```

**For Windows (PowerShell):**

powershell

```powershell
$env:ATLAN_API_KEY="your-atlan-api-key"
$env:ATLAN_BASE_URL="https://your-atlan-instance.com"
```

### Customizing the Script

Before running, update the connection names in the script to match your environment:

python

```python
POSTGRES_CONNECTION_NAME = "your-postgres-connection"
S3_CONNECTION_NAME = "your-s3-connection"
SNOWFLAKE_CONNECTION_NAME = "your-snowflake-connection"
```

You can also adjust matching thresholds to control matching strictness:

python

```python
# Minimum similarity score (0-100) to consider tables/columns a match
TABLE_MATCH_THRESHOLD = 80  # Lower for more matches, higher for stricter matching
COLUMN_MATCH_THRESHOLD = 80  # Lower for more matches, higher for stricter matching
```

### Execution

Run the script with Python:

bash

```bash
python3 atlan_table_column_lineage_creator.py
```

### Processing Flow

1.  The script connects to Atlan and retrieves connection details
2.  It discovers tables and their columns for each connection
3.  It matches tables between PostgreSQL and S3 based on name similarity
4.  It matches tables between S3 and Snowflake based on name similarity
5.  For each matched table pair, it identifies matching columns
6.  It creates lineage relationships for PostgreSQL → S3 connections (tables and columns)
7.  It creates lineage relationships for S3 → Snowflake connections (tables and columns)

### Key Functions

-   `normalize_name()`: Standardizes names for comparison
-   `calculate_name_similarity()`: Computes similarity scores between names
-   `get_tables_from_connection()`: Retrieves tables for a specific connection
-   `get_columns_for_table()`: Retrieves columns for a specific table
-   `find_matching_tables()`: Identifies table matches between connections
-   `find_matching_columns()`: Identifies column matches between tables
-   `create_table_lineage()`: Creates lineage between tables
-   `create_column_lineage()`: Creates lineage between columns
-   `create_end_to_end_lineage()`: Orchestrates the end-to-end lineage creation process

## Notes

-   The script handles different connection types (PostgreSQL, S3, Snowflake) with appropriate logic
-   Table matches are determined by name similarity after normalization
-   Column matches are determined by name similarity after normalization
-   Lineage relationships are created with descriptive process names
-   The script can handle connections with many tables and columns

## Troubleshooting

-   If Atlan connection fails, check your API key and base URL
-   If tables aren't being discovered, verify connection names are correct
-   If matching isn't working well, adjust the similarity thresholds
-   If lineage creation fails, check permissions in Atlan
-   For error details, check the traceback outputs in the console
-   If lineage relationships already exist, you may see duplicate processes
-   S3 connections require specific handling due to their different structure

## License

[MIT License](https://claude.ai/chat/LICENSE)
