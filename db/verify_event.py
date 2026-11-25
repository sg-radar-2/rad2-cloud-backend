from db.connection import get_connection


def verify_event():
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, kind, situation, prob, label
            FROM events
            WHERE device_id = %s
              AND edge_event_id = %s
            """,
            ("rad2pi", 900003)
        ).fetchone()

        if row is None:
            raise RuntimeError("Test event not found")

        event_id, kind, situation, prob, label = row

        assert event_id == 4
        assert kind == "final"
        assert situation == "반복 접근"
        assert abs(prob - 0.95) < 0.0001
        assert label is None

        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM events
            WHERE device_id = %s
              AND edge_event_id = %s
            """,
            ("rad2pi", 900003)
        ).fetchone()[0]

        assert count == 1

    print("[OK] Event ID:", event_id, flush=True)
    print("[OK] Situation:", situation, flush=True)
    print("[OK] Probability:", prob, flush=True)
    print("[OK] Event count:", count, flush=True)
    print("[SUCCESS] Final Event update verified.", flush=True)


if __name__ == "__main__":
    verify_event()