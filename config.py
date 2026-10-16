"""
RetailPulse Configuration Module
Centralized settings, path definitions, and service connectivity discovery.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory of project
BASE_DIR = Path(__file__).resolve().parent

# Load .env file
load_dotenv(BASE_DIR / ".env")

# Directory Paths
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
ERRORS_DATA_DIR = DATA_DIR / "errors"
DELTA_LAKE_DIR = DATA_DIR / "delta_tables"
MONGO_STORE_DIR = DATA_DIR / "mongo_store"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
SQL_DIR = BASE_DIR / "sql"

for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, ERRORS_DATA_DIR, 
                  DELTA_LAKE_DIR, MONGO_STORE_DIR, LOGS_DIR, REPORTS_DIR, SQL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Application Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database (PostgreSQL with SQLite fallback)
DB_TYPE = os.getenv("DB_TYPE", "auto")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retailpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retailpulse123")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retailpulse_db")

SQLITE_PATH = DATA_DIR / "retailpulse.db"

# Construct default PostgreSQL URL (using psycopg2 driver)
POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
DATABASE_URL = os.getenv("DATABASE_URL", POSTGRES_URL)

# NoSQL (MongoDB)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "retailpulse_nosql")

# Message Broker (RabbitMQ)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "retail_sales_events")
RABBITMQ_DLQ = os.getenv("RABBITMQ_DLQ", "retail_sales_events_dlq")

# Webhook Security
WOOCOMMERCE_WEBHOOK_SECRET = os.getenv("WOOCOMMERCE_WEBHOOK_SECRET", "whsec_retailpulse_secret_key_2026")

# Server Ports
FLASK_HOST = os.getenv("FLASK_HOST", "127.0.0.1")
FLASK_PORT = int(os.getenv("FLASK_PORT", "5000"))
STREAMLIT_PORT = int(os.getenv("STREAMLIT_PORT", "8501"))

# Delta Lake Path
DELTA_TABLE_PATH = BASE_DIR / os.getenv("DELTA_LAKE_PATH", "data/delta_tables/sales_delta")

# Elasticsearch
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "retailpulse_orders")
