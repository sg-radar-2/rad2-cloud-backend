"""RAD-2 LIVE relay: one Render process, one Pi device, many Flutter subscribers.

WS /ws/device/live : Raspberry Pi (X-Device-Key) -> Cloud
WS /ws/live        : Cloud -> Flutter (?token=app bearer token)
GET /api/status   : existing app_api uses hub.status_overlay(device_id)

Frame data stays in process memory; never stored in PostgreSQL.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app_api import DEVICE_ID
from db.connection import get_connection

log = logging.getLogger("rad2.live")
router = APIRouter(tags=["live"])

# LIVE traffic is lightweight, bounded, and not written to PostgreSQL.
MAX_MESSAGE_BYTES = 64 * 1024
MAX_PEOPLE = 50
FRAME_STALE_SECONDS = 2.5
STATUS_STALE_SECONDS = 15.0
HEARTBEAT_SECONDS = 1.0


class PersonFrame(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(min_length=1, max_length=128)
    x: float = Field(allow_inf_nan=False)
    y: float = Field(allow_inf_nan=False)
    hidden: bool = False
    situation: str = "판단 중"
    prob: float = Field(default=0, ge=0, le=1, allow_inf_nan=False)
    abnormal: bool = False
    still_s: float = Field(default=0, ge=0, allow_inf_nan=False)
    elapsed_s: float = Field(default=0, ge=0, allow_inf_nan=False)


class FrameMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["frame"]
    t: datetime
    radar: str = Field(min_length=1, max_length=64)
    people: list[PersonFrame] = Field(max_length=MAX_PEOPLE)


# Only forward known status properties to Flutter's GET /api/status.
STATUS_KEYS = {
    "radar", "radar_host", "radar_error", "frame_rate", "camera",
    "camera_backend", "latency_s", "judge_error", "cpu_temp_c",
    "uptime_s", "storage", "pending_photos",
}


@dataclass
class DeviceRuntime:
    connected: bool = False
    generation: int = 0
    frame: dict[str, Any] | None = None
    frame_at: float = 0.0
    status: dict[str, Any] | None = None
    status_at: float = 0.0
    subscribers: set[asyncio.Queue[dict[str, Any]]] = field(default_factory=set)


class LiveHub:
    """All state is local to a single Uvicorn worker/event loop.

    Synchronous reads from FastAPI's GET /api/status only copy immutable-shaped
    snapshots; mutations happen on the event loop without intervening await.
    """

    def __init__(self) -> None:
        self._devices: dict[str, DeviceRuntime] = {}

    def _state(self, device_id: str) -> DeviceRuntime:
        return self._devices.setdefault(device_id, DeviceRuntime())

    def _offer(self, queue: asyncio.Queue, message: dict[str, Any]) -> None:
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        queue.put_nowait(message)

    def _broadcast(self, state: DeviceRuntime, message: dict[str, Any]) -> None:
        for q in tuple(state.subscribers):
            self._offer(q, message)

    @staticmethod
    def _empty_frame(radar: str) -> dict[str, Any]:
        return {
            "type": "frame",
            "t": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "radar": radar,
            "people": [],
        }

    def attach_device(self, device_id: str) -> int:
        st = self._state(device_id)
        st.generation += 1
        st.connected = True
        # Do not show an old Pi session's people on new connection.
        st.frame = None
        st.frame_at = 0.0
        st.status = None
        st.status_at = 0.0
        self._broadcast(st, self._empty_frame("connecting"))
        return st.generation

    def is_current(self, device_id: str, generation: int) -> bool:
        st = self._state(device_id)
        return st.connected and st.generation == generation

    def detach_device(self, device_id: str, generation: int) -> None:
        st = self._state(device_id)
        if st.generation != generation:
            return
        st.connected = False
        st.frame = None
        st.frame_at = 0.0
        st.status = None
        st.status_at = 0.0
        self._broadcast(st, self._empty_frame("disconnected"))

    def publish_frame(self, device_id: str, frame: dict[str, Any]) -> None:
        st = self._state(device_id)
        st.frame = frame
        st.frame_at = time.monotonic()
        self._broadcast(st, frame)

    def publish_status(self, device_id: str, status: dict[str, Any]) -> None:
        st = self._state(device_id)
        st.status = status
        st.status_at = time.monotonic()

    def frame_snapshot(self, device_id: str) -> dict[str, Any]:
        st = self._state(device_id)
        if not st.connected:
            return self._empty_frame("disconnected")
        if st.frame is None:
            return self._empty_frame("connecting")
        if time.monotonic() - st.frame_at > FRAME_STALE_SECONDS:
            return self._empty_frame("stale")
        return st.frame

    def status_overlay(self, device_id: str) -> dict[str, Any]:
        """Recent Pi status + effective connection state, for GET /api/status."""
        st = self._state(device_id)
        now = time.monotonic()
        recent_status = (
            st.connected and st.status is not None
            and now - st.status_at <= STATUS_STALE_SECONDS
        )
        recent_frame = (
            st.connected and st.frame is not None
            and now - st.frame_at <= FRAME_STALE_SECONDS
        )
        overlay = dict(st.status) if recent_status else {}
        if recent_frame:
            overlay["radar"] = st.frame.get("radar", "ok")
        elif st.connected:
            overlay["radar"] = "stale" if st.frame is not None else "connecting"
        else:
            overlay["radar"] = "disconnected"
        if not recent_status:
            # Avoid presenting old Pi CPU/temp/camera stats as current.
            for key in STATUS_KEYS - {"radar"}:
                overlay.pop(key, None)
        overlay["status_source"] = "pi" if recent_status else "unavailable"
        return overlay

    def subscribe(self, device_id: str) -> asyncio.Queue[dict[str, Any]]:
        st = self._state(device_id)
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        st.subscribers.add(q)
        self._offer(q, self.frame_snapshot(device_id))
        return q

    def unsubscribe(self, device_id: str, q: asyncio.Queue) -> None:
        self._state(device_id).subscribers.discard(q)


hub = LiveHub()


def _validate_device_key(device_id: str, device_key: str | None) -> bool:
    """Match current capstone's ONE-device env-key policy and DB registration."""
    expected = os.getenv("RAD2_DEVICE_API_KEY")
    if not expected or not device_key or device_id != DEVICE_ID:
        return False
    if not secrets.compare_digest(device_key, expected):
        return False
    with get_connection() as conn:
        return conn.execute(
            "SELECT 1 FROM devices WHERE device_id = %s", (device_id,)
        ).fetchone() is not None


def _device_for_app_token(token: str) -> str | None:
    if not token:
        return None
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with get_connection() as conn:
        row = conn.execute(
            """SELECT device_id FROM app_tokens
               WHERE token_hash = %s AND revoked_at IS NULL""",
            (token_hash,),
        ).fetchone()
    return str(row[0]) if row else None


@router.websocket("/ws/device/live")
async def ws_device_live(websocket: WebSocket) -> None:
    device_id = websocket.query_params.get("device_id", "")
    device_key = websocket.headers.get("x-device-key")
    try:
        valid = await asyncio.to_thread(_validate_device_key, device_id, device_key)
    except Exception:
        log.exception("Device WebSocket authentication DB unavailable")
        await websocket.close(code=1011)
        return
    if not valid:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    generation = hub.attach_device(device_id)
    log.info("LIVE device connected: %s", device_id)
    try:
        while True:
            text = await websocket.receive_text()
            if not hub.is_current(device_id, generation):
                await websocket.close(code=1012)
                return
            if len(text.encode("utf-8")) > MAX_MESSAGE_BYTES:
                await websocket.close(code=1009)
                return
            try:
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    continue
                msg_type = payload.get("type")
                if msg_type == "frame":
                    validated = FrameMessage.model_validate(payload)
                    frame = validated.model_dump(mode="json")
                    # Keep the Pi's original ISO representation if present.
                    frame["t"] = payload["t"]
                    hub.publish_frame(device_id, frame)
                elif msg_type == "status":
                    if payload.get("device_id", device_id) != device_id:
                        continue
                    safe = {k: v for k, v in payload.items() if k in STATUS_KEYS}
                    if not isinstance(safe.get("radar"), str):
                        continue
                    if len(json.dumps(safe, ensure_ascii=False)) > MAX_MESSAGE_BYTES:
                        continue
                    hub.publish_status(device_id, safe)
                # Unknown messages are ignored; Pi Event JSON uses existing REST.
            except (ValueError, TypeError, ValidationError):
                log.warning("Ignoring malformed LIVE message from %s", device_id)
    except WebSocketDisconnect:
        pass
    finally:
        hub.detach_device(device_id, generation)
        log.info("LIVE device disconnected: %s", device_id)


async def _sender(websocket: WebSocket, device_id: str, queue: asyncio.Queue) -> None:
    """Never let a slow phone block Pi reception or other subscribers."""
    while True:
        try:
            frame = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
            # If a queued frame is now old, clear people rather than replay it.
            if frame.get("people") and hub.frame_snapshot(device_id).get("radar") == "stale":
                frame = hub.frame_snapshot(device_id)
        except asyncio.TimeoutError:
            frame = hub.frame_snapshot(device_id)
        await asyncio.wait_for(websocket.send_json(frame), timeout=2.0)


async def _reader(websocket: WebSocket) -> None:
    # Flutter currently only receives; reading detects disconnects promptly.
    while True:
        await websocket.receive_text()


@router.websocket("/ws/live")
async def ws_flutter_live(websocket: WebSocket) -> None:
    # Existing Flutter client uses ?token= . Do not log/query-dump this value.
    token = websocket.query_params.get("token", "")
    try:
        device_id = await asyncio.to_thread(_device_for_app_token, token)
    except Exception:
        log.exception("App WebSocket authentication DB unavailable")
        await websocket.close(code=1011)
        return
    if not device_id:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    q = hub.subscribe(device_id)
    sender = asyncio.create_task(_sender(websocket, device_id, q))
    reader = asyncio.create_task(_reader(websocket))
    try:
        done, pending = await asyncio.wait({sender, reader}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            try:
                await task
            except (WebSocketDisconnect, asyncio.CancelledError, OSError, RuntimeError):
                pass
            except asyncio.TimeoutError:
                pass
    finally:
        hub.unsubscribe(device_id, q)
