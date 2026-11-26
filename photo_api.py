"""RAD-2 M7: PostgreSQL-only JPEG photo endpoints.

This module is intended to be copied into rad2-cloud-backend/photo_api.py.
It reuses existing app_api.py authorization and db.connection.get_connection().

Pi sends raw JPEG bytes with X-Device-Key, addressing edge_event_id.
Flutter downloads by Cloud events.id, with Bearer token.
JPEG contents live in PostgreSQL event_photos.image_data BYTEA.

Database migration: db/m7_photo_schema.sql (before registering this router).
"""
from __future__ import annotations

import hashlib
import os
import secrets

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from pydantic import BaseModel, Field

from app_api import ApiError, DEVICE_ID, require_app_device
from db.connection import get_connection

router = APIRouter(tags=["photos"])

# Demo limits: measured original ~=1.5 MB; allow existing files before compression.
MAX_ORIGINAL_BYTES = 3 * 1024 * 1024
MAX_THUMB_BYTES = 512 * 1024
KEEP_DAYS = 30  # Pending list window only; NOT automatic deletion.


def _device_auth(x_device_key: str | None) -> str:
    expected = os.getenv("RAD2_DEVICE_API_KEY")
    if not expected:
        raise ApiError(503, "device_auth_not_configured", "Device Key가 설정되지 않았습니다.")
    if not x_device_key or not secrets.compare_digest(x_device_key, expected):
        raise ApiError(401, "unauthorized", "Device Key가 올바르지 않습니다.")
    # Current capstone deployment supports exactly one Pi: rad2pi.
    return DEVICE_ID


async def _receive_jpeg(request: Request, max_bytes: int) -> bytes:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "image/jpeg":
        raise ApiError(415, "unsupported_media_type", "Content-Type: image/jpeg가 필요합니다.")
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise ApiError(413, "photo_too_large", "JPEG 크기 제한을 초과했습니다.")
        except ValueError:
            raise ApiError(400, "bad_content_length", "잘못된 Content-Length입니다.")

    chunks = bytearray()
    async for chunk in request.stream():
        chunks.extend(chunk)
        if len(chunks) > max_bytes:
            raise ApiError(413, "photo_too_large", "JPEG 크기 제한을 초과했습니다.")
    jpg = bytes(chunks)
    # Fast format sanity check; not a full image-decoder verification.
    if len(jpg) < 4 or not jpg.startswith(b"\xff\xd8") or not jpg.endswith(b"\xff\xd9"):
        raise ApiError(400, "invalid_jpeg", "JPEG 시작/종료 마커가 올바르지 않습니다.")
    return jpg


@router.post("/api/device/events/{edge_event_id}/photo")
async def upload_photo(
    edge_event_id: int,
    request: Request,
    thumb: int = Query(default=0, ge=0, le=1),
    x_device_key: str | None = Header(default=None, alias="X-Device-Key"),
) -> dict:
    """Save/replace one JPEG; same byte retries are idempotent and keep ACKs."""
    device_id = _device_auth(x_device_key)
    if edge_event_id < 1:
        raise ApiError(400, "bad_event_id", "edge_event_id는 양수여야 합니다.")
    variant = "thumb" if thumb else "original"
    jpg = await _receive_jpeg(
        request,
        MAX_THUMB_BYTES if thumb else MAX_ORIGINAL_BYTES,
    )
    sha256 = hashlib.sha256(jpg).hexdigest()

    with get_connection() as conn:
        # Lock the event row: conflicting concurrent retries serialize safely.
        row = conn.execute(
            """SELECT id FROM events
               WHERE device_id = %s AND edge_event_id = %s
               FOR UPDATE""",
            (device_id, edge_event_id),
        ).fetchone()
        if row is None:
            raise ApiError(404, "not_found", "사진보다 Event를 먼저 전송해야 합니다.")
        cloud_event_id = int(row[0])
        old = conn.execute(
            "SELECT sha256 FROM event_photos WHERE event_id = %s AND variant = %s",
            (cloud_event_id, variant),
        ).fetchone()
        changed = old is None or old[0] != sha256
        if changed:
            conn.execute(
                """INSERT INTO event_photos
                       (event_id, variant, image_data, sha256, size_bytes)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (event_id, variant) DO UPDATE SET
                           image_data = EXCLUDED.image_data,
                           sha256 = EXCLUDED.sha256,
                           size_bytes = EXCLUDED.size_bytes,
                           uploaded_at = NOW()""",
                (cloud_event_id, variant, jpg, sha256, len(jpg)),
            )
            # The phone stores the original JPEG.  Thumbnail changes alone do
            # not invalidate an already downloaded original.
            if variant == "original":
                conn.execute("DELETE FROM photo_acks WHERE event_id = %s", (cloud_event_id,))
        if variant == "original":
            # photo_key is a descriptive logical reference, not a filesystem path.
            conn.execute(
                """UPDATE events SET photo = TRUE, photo_key = %s, updated_at = NOW()
                   WHERE id = %s AND device_id = %s""",
                (f"pg:event_photos/{cloud_event_id}/original", cloud_event_id, device_id),
            )
    return {
        "ok": True,
        "id": cloud_event_id,
        "edge_event_id": edge_event_id,
        "thumb": bool(thumb),
        "bytes": len(jpg),
        "changed": changed,
    }


@router.get("/api/events/{event_id}/photo")
def download_photo(
    event_id: int,
    thumb: int = Query(default=0, ge=0, le=1),
    device_id: str = Depends(require_app_device),
) -> Response:
    """Return private JPEG bytes to a paired app; thumb falls back to original."""
    with get_connection() as conn:
        event = conn.execute(
            "SELECT id FROM events WHERE id = %s AND device_id = %s",
            (event_id, device_id),
        ).fetchone()
        if event is None:
            raise ApiError(404, "not_found", "이벤트가 없습니다.")
        variant = "thumb" if thumb else "original"
        row = conn.execute(
            "SELECT image_data FROM event_photos WHERE event_id = %s AND variant = %s",
            (event_id, variant),
        ).fetchone()
        if row is None and thumb:
            row = conn.execute(
                "SELECT image_data FROM event_photos WHERE event_id = %s AND variant = 'original'",
                (event_id,),
            ).fetchone()
    if row is None:
        raise ApiError(404, "no_photo", "이 이벤트에는 업로드된 사진이 없습니다.")
    return Response(
        content=bytes(row[0]),
        media_type="image/jpeg",
        headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"},
    )


def _app_token_id(authorization: str | None, device_id: str) -> int:
    # require_app_device has validated the same Authorization already.
    if not authorization or not authorization.lower().startswith("bearer "):
        raise ApiError(401, "unauthorized", "앱 Token이 필요합니다.")
    token_hash = hashlib.sha256(authorization[7:].strip().encode("utf-8")).hexdigest()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM app_tokens WHERE device_id = %s AND token_hash = %s AND revoked_at IS NULL",
            (device_id, token_hash),
        ).fetchone()
    if row is None:
        raise ApiError(401, "unauthorized", "앱 Token이 올바르지 않습니다.")
    return int(row[0])


@router.get("/api/photos/pending")
def pending_photos(
    authorization: str | None = Header(default=None),
    device_id: str = Depends(require_app_device),
) -> dict:
    """Cloud event IDs with uploaded originals not yet ACKed by this phone."""
    app_token_id = _app_token_id(authorization, device_id)
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT e.id FROM events e
               WHERE e.device_id = %s
                 AND e.t >= NOW() - make_interval(days => %s)
                 AND EXISTS (
                     SELECT 1 FROM event_photos ep
                     WHERE ep.event_id = e.id AND ep.variant = 'original'
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM photo_acks a
                     WHERE a.event_id = e.id AND a.app_token_id = %s
                 )
               ORDER BY e.t ASC, e.id ASC LIMIT 500""",
            (device_id, KEEP_DAYS, app_token_id),
        ).fetchall()
    return {"ids": [int(row[0]) for row in rows]}


class PhotoAckRequest(BaseModel):
    ids: list[int] = Field(default_factory=list, max_length=500)


@router.post("/api/photos/ack")
def ack_photos(
    payload: PhotoAckRequest,
    authorization: str | None = Header(default=None),
    device_id: str = Depends(require_app_device),
) -> dict:
    """ACK only stored originals owned by this device; return cumulative count."""
    app_token_id = _app_token_id(authorization, device_id)
    ids = sorted({v for v in payload.ids if v > 0})
    with get_connection() as conn:
        if ids:
            conn.execute(
                """INSERT INTO photo_acks (app_token_id, event_id)
                   SELECT %s, e.id FROM events e
                   WHERE e.device_id = %s AND e.id = ANY(%s::bigint[])
                     AND EXISTS (SELECT 1 FROM event_photos ep
                                 WHERE ep.event_id = e.id AND ep.variant = 'original')
                   ON CONFLICT (app_token_id, event_id) DO NOTHING""",
                (app_token_id, device_id, ids),
            )
        row = conn.execute(
            """SELECT COUNT(*) FROM photo_acks a
               JOIN events e ON e.id = a.event_id
               WHERE a.app_token_id = %s AND e.device_id = %s""",
            (app_token_id, device_id),
        ).fetchone()
    return {"acked": int(row[0])}
