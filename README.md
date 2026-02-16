# E-commerce ETL Pipeline

## Project Overview
This project implements a robust **ETL (Extract, Transform, Load) Pipeline** built with Python to handle e-commerce transaction data. The goal is to demonstrate the ability to process raw CSV datasets into clean, structured formats suitable for business intelligence and analytical reporting.

## Key Features
* **Automated Data Ingestion**: Extracts raw transaction data from CSV files located in the `data/` directory.
* **Data Transformation**: Cleans and normalizes data using Python logic within the `src/` directory to ensure data integrity.
* **Modular Architecture**: Organized into a professional directory structure (`config/`, `src/`, `data/`) for scalability and maintainability.
* **Configurable Workflows**: Uses a dedicated configuration setup to manage environment variables and file paths.

## Tech Stack
* **Language**: Python.
* **Environment**: Virtual Environments (`.venv`) for dependency isolation.
* **Version Control**: Git.

## Project Structure
```text
ecommerce-etl-pipeline/
├── config/             # Configuration settings and environment variables
├── data/               # Raw and processed CSV datasets
├── src/                # Core Python scripts for ETL logic
├── .gitignore          # Standard Git ignore file
└── README.md           # Project documentation

