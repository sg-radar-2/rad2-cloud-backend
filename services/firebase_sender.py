from pathlib import Path

import firebase_admin
from firebase_admin import credentials, messaging


FIREBASE_KEY = Path("/etc/secrets/firebase.json")


def initialize_firebase():
    """Firebase Admin SDK 초기화."""

    if not firebase_admin._apps:
        credential = credentials.Certificate(str(FIREBASE_KEY))
        firebase_admin.initialize_app(credential)


def send_alert(fcm_token: str, event: dict) -> str:
    """휴대폰 한 대에 이상행동 알림 전송."""

    initialize_firebase()

    message = messaging.Message(
        token=fcm_token,

        notification=messaging.Notification(
            title="이상 상황",
            body=(
                f"{event['situation']} · "
                f"{event['t'][11:16]}"
            ),
        ),

        data={
            "event_id": str(event["id"]),
            "situation": str(event["situation"]),
            "t": str(event["t"]),
        },

        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(
                channel_id="abnormal"
            ),
        ),
    )

    return messaging.send(message)