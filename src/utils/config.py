import os
from dotenv import load_dotenv
load_dotenv()

DB_CONN = DB_CONN = os.getenv(
    "DB_CONN",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/citi"
)
RAW_DIR = os.getenv("RAW_DIR", "data/raw")
