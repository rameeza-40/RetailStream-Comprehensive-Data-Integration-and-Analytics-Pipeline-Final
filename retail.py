"""
RetailStream - Comprehensive Data Integration and Analytics Pipeline - Final
Author: Your Name
"""

import os
import json
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine
from pymongo import MongoClient

# ========================
# CONFIGURATION
# ========================
LOG_FILE = "pipeline.log"
RAW_DATA_DIR = "./raw_data"
PROCESSED_DATA_DIR = "./processed_data"
AGGREGATED_FILE = "./aggregated/retail_summary.csv"

# Example PostgreSQL RDS connection
RDS_CONNECTION = "postgresql://username:password@hostname:5432/retaildb"

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "retailstream"
MONGO_COLLECTION = "transactions"

# ========================
# LOGGING SETUP
# ========================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========================
# INGESTION FUNCTIONS
# ========================
def ingest_csv(file_path: str) -> pd.DataFrame:
    """Read data from CSV"""
    logging.info(f"Ingesting CSV file: {file_path}")
    return pd.read_csv(file_path)

def ingest_json(file_path: str) -> pd.DataFrame:
    """Read data from JSON"""
    logging.info(f"Ingesting JSON file: {file_path}")
    with open(file_path, "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def ingest_api(api_response: dict) -> pd.DataFrame:
    """Simulate API ingestion"""
    logging.info("Ingesting API data...")
    return pd.DataFrame(api_response)

# ========================
# VALIDATION & CLEANING
# ========================
def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Validate schema and clean data"""
    logging.info("Validating and cleaning data...")
    required_cols = ["transaction_id", "customer_id", "product", "price", "quantity", "timestamp"]

    # Ensure all columns exist
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            raise ValueError(f"Missing column: {col}")

    # Drop duplicates and nulls
    df = df.drop_duplicates().dropna()

    # Convert types
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df

# ========================
# STORAGE (MongoDB)
# ========================
def store_raw_in_mongo(df: pd.DataFrame):
    """Store raw JSON records into MongoDB"""
    logging.info("Storing raw data into MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.insert_many(df.to_dict(orient="records"))
    client.close()

# ========================
# PROCESSING (Save CSV/Parquet)
# ========================
def process_and_store(df: pd.DataFrame, output_file: str):
    """Save processed data into CSV"""
    logging.info(f"Storing processed data into {output_file}...")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)

# ========================
# AGGREGATION
# ========================
def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sales by product"""
    logging.info("Aggregating sales data...")
    agg_df = df.groupby("product").agg(
        total_sales=pd.NamedAgg(column="price", aggfunc="sum"),
        total_quantity=pd.NamedAgg(column="quantity", aggfunc="sum"),
        avg_price=pd.NamedAgg(column="price", aggfunc="mean")
    ).reset_index()
    return agg_df

# ========================
# LOAD INTO RDS
# ========================
def load_into_rds(df: pd.DataFrame, table_name: str):
    """Load final aggregated data into RDS (PostgreSQL/MySQL)"""
    logging.info(f"Loading data into RDS table: {table_name}")
    engine = create_engine(RDS_CONNECTION)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    engine.dispose()

# ========================
# MAIN PIPELINE ORCHESTRATOR
# ========================
def main():
    logging.info("Pipeline started...")

    # Step 1: Ingest sample data (CSV)
    csv_file = os.path.join(RAW_DATA_DIR, "transactions.csv")
    df = ingest_csv(csv_file)

    # Step 2: Validate and Clean
    df = validate_and_clean(df)

    # Step 3: Store raw JSON into MongoDB
    store_raw_in_mongo(df)

    # Step 4: Save cleaned data into CSV (simulate S3)
    processed_file = os.path.join(PROCESSED_DATA_DIR, "transactions_cleaned.csv")
    process_and_store(df, processed_file)

    # Step 5: Aggregate Data
    agg_df = aggregate_data(df)
    process_and_store(agg_df, AGGREGATED_FILE)

    # Step 6: Load into RDS
    load_into_rds(agg_df, "retail_summary")

    logging.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()







