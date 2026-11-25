"""RAD-2 Flutter API: pairing, Bearer-token authentication, and event access.

Install at E:\\radar\\rad2-cloud-backend\\app_api.py.
Requires the existing db.connection.get_connection() (psycopg 3) and tables
app_tokens(device_id, token_hash) and events(id, device_id, ...).
"""

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from db.connection import get_connection

router = APIRouter()

DEVICE_ID = "rad2pi"  # Current capstone demo: one registered Raspberry Pi
KST = timezone(timedelta(hours=9))
SITUATIONS = frozenset(
    ("통과", "귀가", "외출", "방문", "배달", "장시간 체류", "배회", "반복 접근")
)
SUMMARY_EXTRA = ("also", "end_situation", "reason", "path")


class ApiError(Exception):
    """Return the same flat error JSON shape as the original Pi API."""

    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message


async def api_error_handler(_request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status,
        content={"error": exc.code, "message": exc.message},
    )


class PairRequest(BaseModel):
    code: str


class LabelRequest(BaseModel):
    # Field(...) means that the key must exist, but the value may be null.
    situation: str | None = Field(...)


@router.post("/api/pair")
def pair_device(request: PairRequest) -> dict[str, str]:
    """Issue one app token; store only its SHA-256 hash in PostgreSQL."""
    expected_code = os.getenv("RAD2_PAIR_CODE")
    if not expected_code:
        raise ApiError(503, "not_configured", "페어링 코드가 설정되지 않았습니다.")
    if not secrets.compare_digest(request.code, expected_code):
        raise ApiError(401, "bad_code", "페어링 코드가 올바르지 않습니다.")

    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO devices (device_id, name)
            VALUES (%s, %s)
            ON CONFLICT (device_id) DO NOTHING
            """,
            (DEVICE_ID, "RAD-2 Raspberry Pi"),
        )
        conn.execute(
            "INSERT INTO app_tokens (device_id, token_hash) VALUES (%s, %s)",
            (DEVICE_ID, token_hash),
        )
    return {"token": token}


def require_app_device(authorization: str | None = Header(default=None)) -> str:
    """Authenticate a Flutter Bearer token and return its paired device ID."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise ApiError(401, "unauthorized", "앱 인증 Token이 필요합니다. 다시 페어링하세요.")
    token = authorization[7:].strip()
    if not token:
        raise ApiError(401, "unauthorized", "앱 인증 Token이 필요합니다. 다시 페어링하세요.")

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT device_id FROM app_tokens WHERE token_hash = %s",
            (token_hash,),
        ).fetchone()
    if row is None:
        raise ApiError(401, "unauthorized", "앱 인증 Token이 올바르지 않습니다.")
    return str(row[0])


def _timestamp(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    return value


def _event_view(row: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    """Map Cloud's details JSONB to the original Pi API's flat event shape."""
    details = row.get("details") or {}
    if not isinstance(details, dict):
        details = {}

    event = {k: v for k, v in row.items() if k != "details"}
    for key in ("t", "started_at", "ended_at"):
        event[key] = _timestamp(event.get(key))
    if event.get("prob") is not None:
        event["prob"] = float(event["prob"])
    event["photo"] = bool(event.get("photo"))

    # The Pi API exposes final-event detail keys at top level.
    if detailed:
        event.update(details)
    else:
        event.update({key: details[key] for key in SUMMARY_EXTRA if key in details})
    return event


EVENT_COLUMNS = """
    id, kind, person, t, started_at, ended_at,
    situation, prob, abnormal, photo, label, details
"""


@router.get("/api/events")
def list_events(
    since: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    abnormal: bool | None = None,
    device_id: str = Depends(require_app_device),
) -> list[dict[str, Any]]:
    """List paired device's events in most-recent-first order."""
    query = f"SELECT {EVENT_COLUMNS} FROM events WHERE device_id = %s"
    params: list[Any] = [device_id]

    if since is not None:
        # Preserve +09:00 if an unencoded '+' turned into a space in the URL.
        try:
            since_dt = datetime.fromisoformat(since.replace(" ", "+"))
        except ValueError:
            raise ApiError(400, "bad_since", "since는 ISO 8601 날짜·시각이어야 합니다.")
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=KST)
        query += " AND t > %s"
        params.append(since_dt)

    if abnormal is not None:
        query += " AND abnormal = %s"
        params.append(abnormal)

    query += " ORDER BY t DESC, id DESC LIMIT %s"
    params.append(limit)

    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return [_event_view(row) for row in rows]


@router.get("/api/events/{event_id}")
def get_event(
    event_id: int,
    device_id: str = Depends(require_app_device),
) -> dict[str, Any]:
    """Get the complete event, including final behaviors and track."""
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {EVENT_COLUMNS} FROM events WHERE id = %s AND device_id = %s",
                (event_id, device_id),
            )
            row = cur.fetchone()
    if row is None:
        raise ApiError(404, "not_found", f"이벤트 {event_id}가 없습니다.")
    return _event_view(row, detailed=True)


@router.post("/api/events/{event_id}/label")
def update_event_label(
    event_id: int,
    request: LabelRequest,
    device_id: str = Depends(require_app_device),
) -> dict[str, Any]:
    """Set a situation label, or clear it with {\"situation\": null}."""
    if request.situation is not None and request.situation not in SITUATIONS:
        raise ApiError(400, "bad_situation", "선택할 수 없는 상황입니다.")

    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE events
                SET label = %s, updated_at = NOW()
                WHERE id = %s AND device_id = %s
                RETURNING {EVENT_COLUMNS}
                """,
                (request.situation, event_id, device_id),
            )
            row = cur.fetchone()
    if row is None:
        raise ApiError(404, "not_found", f"이벤트 {event_id}가 없습니다.")
    return _event_view(row)


class PushTokenRequest(BaseModel):
    token: str = Field(min_length=1, max_length=4096)
    platform: Literal["android", "ios"] = "android"


@router.put("/api/push_token")
def register_push_token(
    request: PushTokenRequest,
    authorization: str | None = Header(default=None),
    device_id: str = Depends(require_app_device),
):
    """Flutter 앱의 FCM Token 등록 또는 교체."""

    # 1. 앱 인증 Token의 Hash 계산
    app_token = authorization[7:].strip()
    token_hash = hashlib.sha256(
        app_token.encode("utf-8")
    ).hexdigest()

    with get_connection() as conn:

        # 2. 인증된 앱의 DB ID 확인
        row = conn.execute(
            """
            SELECT id
            FROM app_tokens
            WHERE token_hash = %s
              AND device_id = %s
              AND revoked_at IS NULL
            """,
            (token_hash, device_id),
        ).fetchone()

        if row is None:
            raise ApiError(
                401, "unauthorized",
                "앱 인증 Token이 올바르지 않습니다."
            )

        app_token_id = row[0]

        # 3. 같은 앱의 기존 Token을 교체
        #    같은 FCM Token이 다른 등록에 있으면 이전 등록도 정리
        conn.execute(
            """
            DELETE FROM push_tokens
            WHERE app_token_id = %s
               OR fcm_token = %s
            """,
            (app_token_id, request.token),
        )

        # 4. 새로운 FCM Token 저장
        conn.execute(
            """
            INSERT INTO push_tokens (
                app_token_id,
                fcm_token,
                platform
            )
            VALUES (%s, %s, %s)
            """,
            (
                app_token_id,
                request.token,
                request.platform,
            ),
        )

    return {
        "ok": True,
        "platform": request.platform,
    }
