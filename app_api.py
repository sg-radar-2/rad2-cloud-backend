
import hashlib
import os
import secrets

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from db.connection import get_connection


router = APIRouter()


class PairRequest(BaseModel):
    code: str


@router.post("/api/pair")
def pair_device(request: PairRequest):

    # 1. Render에 등록된 페어링 코드 가져오기
    expected_code = os.environ.get("RAD2_PAIR_CODE")

    if not expected_code:
        return JSONResponse(
            status_code=503,
            content={
                "error": "not_configured",
                "message": "페어링 코드가 설정되지 않았습니다."
            }
        )

    # 2. Flutter가 보낸 코드 확인
    if not secrets.compare_digest(
        request.code,
        expected_code
    ):
        return JSONResponse(
            status_code=401,
            content={
                "error": "bad_code",
                "message": "페어링 코드가 올바르지 않습니다."
            }
        )

    # 3. 앱 인증 Token 생성
    token = secrets.token_urlsafe(32)

    # DB에는 Token 원문 대신 Hash 저장
    token_hash = hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()

    # 4. PostgreSQL에 Token 저장
    with get_connection() as conn:

        conn.execute(
            """
            INSERT INTO devices (device_id, name)
            VALUES (%s, %s)
            ON CONFLICT (device_id) DO NOTHING
            """,
            ("rad2pi", "RAD-2 Raspberry Pi")
        )

        conn.execute(
            """
            INSERT INTO app_tokens (
                device_id,
                token_hash
            )
            VALUES (%s, %s)
            """,
            ("rad2pi", token_hash)
        )

    # 5. Flutter에 Token 반환
    return {"token": token}
