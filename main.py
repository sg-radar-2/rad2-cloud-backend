
import logging
import os

import psycopg

from contextlib import asynccontextmanager
from fastapi import FastAPI

from db.connection import check_connection
from device_events import router as device_router
from app_api import router as app_router

logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app: FastAPI):

    # 서버 시작 시 PostgreSQL 연결 확인
    if not os.environ.get("DATABASE_URL"):
        logger.warning("PostgreSQL check skipped: DATABASE_URL not set")
    else:
        try:
            if check_connection():
                logger.info("PostgreSQL connection OK (SELECT 1)")
            else:
                logger.error("PostgreSQL check FAILED")

        except (psycopg.Error, RuntimeError):
            logger.error("PostgreSQL connection FAILED")

    yield


app = FastAPI(
    title="RAD-2 Cloud Backend",
    version="0.1.0",
    lifespan=lifespan
)
app.include_router(device_router)
app.include_router(app_router)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "rad2-cloud"
    }
