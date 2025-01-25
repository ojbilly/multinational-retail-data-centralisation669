# Data Handling Project

## Table of Contents
1. [Project Description](#project-description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Project Description

This project is designed to handle data extraction, cleaning, and uploading processes from various sources, including databases, APIs, PDFs, and S3 storage. The aim is to ensure data integrity and usability by performing the following tasks:
- Extracting data from RDS databases, APIs, PDFs, and S3.
- Cleaning data to remove inconsistencies, NULL values, and formatting errors.
- Uploading cleaned data into a target database for further use.

### What I Learned
- Implementing data cleaning processes with `pandas`.
- Utilizing `SQLAlchemy` to manage database connections and operations.
- Automating API interactions and extracting structured data from JSON and PDF formats.
- Working with AWS S3 for data storage and retrieval.
- Using YAML files to manage sensitive credentials securely.

## Installation

To run this project, follow these steps:

1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/data-handling-project.git
   cd data-handling-project
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up database credentials:
   - Place your database credentials in the `db_creds.yaml` file. Ensure the format matches:
     ```yaml
     source_db:
       RDS_HOST: <source-host>
       RDS_USER: <source-user>
       RDS_PASSWORD: <source-password>
       RDS_DATABASE: <source-database>
       RDS_PORT: <source-port>

     target_db:
       RDS_HOST: <target-host>
       RDS_USER: <target-user>
       RDS_PASSWORD: <target-password>
       RDS_DATABASE: <target-database>
       RDS_PORT: <target-port>
     ```

## Usage

1. **Extract Data**
   Use the `data_extraction.py` script to retrieve data from various sources, including databases, APIs, and S3.

2. **Clean Data**
   Utilize the `data_cleaning.py` script to clean and process extracted data. Various functions handle specific cleaning needs such as handling NULL values, standardizing weights, and fixing date formats.

3. **Upload Cleaned Data**
   Run the `upload_cleaned_data.py` script to upload cleaned data to the target database.

   Example usage:
   ```bash
   python upload_cleaned_data.py
   ```

## File Structure

```plaintext
├── data_cleaning.py          # Functions for cleaning datasets
├── data_extraction.py        # Functions for extracting data from various sources
├── database_utils.py         # Database connection and utility functions
├── db_creds.yaml             # YAML file for storing database credentials
├── upload_cleaned_data.py    # Script to upload cleaned data to the target database
└── requirements.txt          # List of dependencies
```

## License

This project created by Osaze Omoruyi.
