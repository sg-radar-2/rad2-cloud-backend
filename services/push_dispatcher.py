"""M6: Send one FCM alert per camera event and paired app token.

Install as services/push_dispatcher.py. Requires push_deliveries table,
existing app_tokens/push_tokens/events/settings, and services.firebase_sender.send_alert.
"""

import logging
from datetime import datetime, timedelta, timezone

from psycopg.rows import dict_row

from db.connection import get_connection
from services.firebase_sender import send_alert

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
DEFAULT_NOTIFY = ["장시간 체류", "배회", "반복 접근"]


def dispatch_camera_push(cloud_event_id: int) -> None:
    """Called after event transaction commits; failures must not reject Pi uploads."""
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT e.id, e.device_id, e.kind, e.situation, e.t,
                       e.abnormal, s.notify
                FROM events e
                LEFT JOIN settings s ON s.device_id = e.device_id
                WHERE e.id = %s
                """,
                (cloud_event_id,),
            )
            event = cur.fetchone()
            if not event or event["kind"] != "camera" or not event["abnormal"]:
                return

            notify = event["notify"] if event["notify"] is not None else DEFAULT_NOTIFY
            if event["situation"] not in notify:
                return

            cur.execute(
                """
                SELECT pt.app_token_id, pt.fcm_token
                FROM push_tokens pt
                JOIN app_tokens at ON at.id = pt.app_token_id
                WHERE at.device_id = %s AND at.revoked_at IS NULL
                """,
                (event["device_id"],),
            )
            targets = cur.fetchall()

    if not targets:
        log.info("Push skipped: no registered FCM tokens for device %s", event["device_id"])
        return

    event_time = event["t"]
    if isinstance(event_time, datetime):
        event_time = event_time.astimezone(KST).isoformat(timespec="milliseconds")
    else:
        event_time = str(event_time)

    message_event = {
        "id": event["id"],  # Cloud ID; Flutter uses this for event detail
        "situation": event["situation"],
        "t": event_time,
    }

    for target in targets:
        app_token_id = target["app_token_id"]

        # Atomic deduplication per Event + paired App Token.
        try:
            with get_connection() as conn:
                claim = conn.execute(
                    """
                    INSERT INTO push_deliveries(event_id, app_token_id)
                    VALUES (%s, %s)
                    ON CONFLICT (event_id, app_token_id) DO NOTHING
                    RETURNING event_id
                    """,
                    (cloud_event_id, app_token_id),
                ).fetchone()
        except Exception:
            log.exception("Could not reserve push delivery for event=%s", cloud_event_id)
            continue

        if claim is None:
            continue  # Already reserved/sent for this app; do not push again.

        try:
            fcm_message_id = send_alert(target["fcm_token"], message_event)
        except Exception:
            log.exception("FCM send failed: event=%s app_token_id=%s", cloud_event_id, app_token_id)
            # Allow a subsequent Pi retry to retry this failed delivery.
            try:
                with get_connection() as conn:
                    conn.execute(
                        """
                        DELETE FROM push_deliveries
                        WHERE event_id = %s AND app_token_id = %s AND sent_at IS NULL
                        """,
                        (cloud_event_id, app_token_id),
                    )
            except Exception:
                log.exception("Could not release failed push delivery claim")
        else:
            with get_connection() as conn:
                conn.execute(
                    """
                    UPDATE push_deliveries SET sent_at = NOW()
                    WHERE event_id = %s AND app_token_id = %s
                    """,
                    (cloud_event_id, app_token_id),
                )
            log.info("FCM sent: event=%s app_token_id=%s message=%s", cloud_event_id, app_token_id, fcm_message_id)
