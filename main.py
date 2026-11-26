
import logging
import os
import re

import psycopg

from contextlib import asynccontextmanager
from fastapi import FastAPI

from db.connection import check_connection
from device_events import router as device_router
from app_api import router as app_router, ApiError, api_error_handler
from photo_api import router as photo_router
from live_api import router as live_router

logger = logging.getLogger("uvicorn.error")


class _WsTokenRedactor(logging.Filter):
    """Reduce exposure of Flutter ?token= in Uvicorn access/error logs.

    Reverse proxies or external platforms may have separate access logs;
    this does not replace an eventual move to header/subprotocol auth.
    """
    _pattern = re.compile(r"([?&]token=)[^&\s\"']+", re.IGNORECASE)

    def filter(self, record: logging.LogRecord) -> bool:
        rendered = record.getMessage()
        redacted = self._pattern.sub(r"\1[REDACTED]", rendered)
        if redacted != rendered:
            record.msg = redacted
            record.args = ()
        return True


for _logger_name in ("uvicorn.access", "uvicorn.error"):
    logging.getLogger(_logger_name).addFilter(_WsTokenRedactor())


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
app.add_exception_handler(ApiError, api_error_handler)
app.include_router(photo_router)
app.include_router(live_router)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "rad2-cloud"
    }
