import os
import secrets
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb
from services.push_dispatcher import dispatch_camera_push

from db.connection import get_connection

router = APIRouter()


class DeviceEvent(BaseModel):
    device_id: str
    edge_event_id: int = Field(gt=0)
    kind: Literal["camera", "final"]
    person: str = Field(min_length=1)
    t: datetime
    situation: str = Field(min_length=1)
    prob: float = Field(ge=0, le=1)
    abnormal: bool

    started_at: datetime | None = None
    ended_at: datetime | None = None
    photo: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


@router.post("/api/device/events")
def receive_event(
    event: DeviceEvent,
    background_tasks: BackgroundTasks,
    x_device_key: str | None = Header(default=None),
):
    # 1. Raspberry Pi 인증
    expected_key = os.environ.get("RAD2_DEVICE_API_KEY")

    if not expected_key:
        raise HTTPException(503, "Device authentication not configured")

    if not x_device_key or not secrets.compare_digest(
        x_device_key, expected_key
    ):
        raise HTTPException(401, "Invalid device key")

    # 현재 시연 장치는 rad2pi 한 대
    if event.device_id != "rad2pi":
        raise HTTPException(403, "Unknown device")

    # 2. DB 저장
    with get_connection() as conn:

        # 테스트 단계: 인증된 rad2pi 기기 자동 등록
        conn.execute(
            """
            INSERT INTO devices (device_id, name)
            VALUES (%s, %s)
            ON CONFLICT (device_id) DO NOTHING
            """,
            (event.device_id, "RAD-2 Raspberry Pi"),
        )

        # 3. Event 저장 또는 기존 Event 갱신
        row = conn.execute(
            """
            INSERT INTO events (
                device_id, edge_event_id,
                kind, person, t,
                started_at, ended_at,
                situation, prob, abnormal,
                photo, details
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (device_id, edge_event_id)
            DO UPDATE SET
                person = EXCLUDED.person,
                t = EXCLUDED.t,
                started_at = COALESCE(
                    events.started_at,
                    EXCLUDED.started_at
                ),
                ended_at = EXCLUDED.ended_at,
                situation = EXCLUDED.situation,
                prob = EXCLUDED.prob,
                abnormal = EXCLUDED.abnormal,
                details = EXCLUDED.details,
                updated_at = NOW()
            RETURNING id
            """,
            (
                event.device_id,
                event.edge_event_id,
                event.kind,
                event.person,
                event.t,
                event.started_at,
                event.ended_at,
                event.situation,
                event.prob,
                event.abnormal,
                False,  # Cloud 사진 업로드는 아직 미구현
                Jsonb(event.details),
            ),
        ).fetchone()

        cloud_event_id = row[0]

        # camera Event만 FCM 발송 대상으로 처리
    if event.kind == "camera":
        background_tasks.add_task(
            dispatch_camera_push,
            cloud_event_id,
        )
        
    # 4. 저장 성공 응답
    return {
        "ok": True,
        "id": cloud_event_id,
        "device_id": event.device_id,
        "edge_event_id": event.edge_event_id,
        "kind": event.kind,
    }