"""Settings come from environment variables, so the same code runs natively and in Docker."""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/flash_auction")
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
INSTANCE = os.getenv("INSTANCE", "backend")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
