
import sys

from db.connection import get_connection


DEVICE_ID = "rad2pi-m2-test"
EDGE_EVENT_ID = 900001


def run_test(mode: str):
    if mode not in ("seed", "verify"):
        raise ValueError("mode must be seed or verify")

    with get_connection() as conn:

        # 추가 Table 존재 여부 확인
        tables = conn.execute("""
            SELECT
                to_regclass('public.app_tokens'),
                to_regclass('public.photo_acks'),
                to_regclass('public.settings'),
                to_regclass('public.push_tokens')
        """).fetchone()

        if tables is None or any(t is None for t in tables):
            raise RuntimeError("Some tables are missing")

        print("[OK] All new tables exist.", flush=True)

        if mode == "seed":

            # 테스트 기기 등록
            conn.execute("""
                INSERT INTO devices (device_id, name)
                VALUES (%s, %s)
                ON CONFLICT (device_id) DO NOTHING
            """, (DEVICE_ID, "M2 Test Device"))

            # 테스트 이벤트 등록
            conn.execute("""
                INSERT INTO events (
                    device_id,
                    edge_event_id,
                    kind,
                    person,
                    t,
                    situation,
                    prob,
                    abnormal,
                    photo,
                    label
                )
                VALUES (
                    %s, %s, %s, %s,
                    NOW(), %s, %s, %s, %s, %s
                )
                ON CONFLICT (device_id, edge_event_id)
                DO NOTHING
            """, (
                DEVICE_ID,
                EDGE_EVENT_ID,
                "final",
                "T3+7",
                "배회",
                0.87,
                True,
                False,
                None
            ))

        # 저장된 이벤트 조회
        event = conn.execute("""
            SELECT id, kind, situation, label
            FROM events
            WHERE device_id = %s
              AND edge_event_id = %s
        """, (DEVICE_ID, EDGE_EVENT_ID)).fetchone()

        if event is None:
            raise RuntimeError("Test event not found")

        if event[3] is not None:
            raise RuntimeError("label must be NULL")

        count = conn.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE device_id = %s
              AND edge_event_id = %s
        """, (DEVICE_ID, EDGE_EVENT_ID)).fetchone()[0]

        if count != 1:
            raise RuntimeError("Duplicate event detected")

    print(f"[OK] Event ID: {event[0]}", flush=True)
    print(f"[OK] Situation: {event[2]}", flush=True)
    print("[OK] Label is NULL.", flush=True)
    print("[OK] Duplicate count = 1.", flush=True)
    print("[SUCCESS] Database smoke test passed.", flush=True)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    run_test(mode)
