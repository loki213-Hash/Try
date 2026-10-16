import sys
import os
import asyncio

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.database.models import Base
from app.database.repository import DatabaseManager
from config import settings
from app.monitoring.logging import logger


async def run_migrations_online() -> None:
    """Initializes tables and executes schema migrations."""
    logger.info("Executing database migrations...")
    db_mgr = DatabaseManager(settings.DATABASE_URL)
    await db_mgr.init_db()
    await db_mgr.close()
    logger.info("Database migrations applied successfully.")


if __name__ == "__main__":
    asyncio.run(run_migrations_online())
