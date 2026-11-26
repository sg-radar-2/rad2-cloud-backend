"""M-LIVE-3: test deployed Cloud with a simulated Pi + Flutter, no actual Radar.

Requirements: pip install websockets (also included in uvicorn[standard]).
Run in PowerShell: python ./tests/mock_live_cloud.py
Prompts for existing Render Device Key and paired App Token.
"""
import asyncio
import getpass
import json
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

BASE = "wss://rad2-cloud.onrender.com"
HTTP = "https://rad2-cloud.onrender.com"
DEVICE = "rad2pi"


async def until(ws, predicate, timeout=8.0):
    async with asyncio.timeout(timeout):
        while True:
            msg = json.loads(await ws.recv())
            if predicate(msg):
                return msg


def status_from_http(app_token):
    req = Request(HTTP + "/api/status", headers={"Authorization": "Bearer " + app_token})
    with urlopen(req, timeout=20) as response:
        return json.load(response)


async def main():
    print("M-LIVE Cloud relay test (mock Pi, no actual Radar)")
    device_key = input("Render RAD2_DEVICE_API_KEY: ").strip()
    app_token = input("Paired Flutter App Token: ").strip()
    if not device_key or not app_token:
        raise SystemExit("Keys are required")

    app_url = BASE + "/ws/live?token=" + quote(app_token, safe="")
    pi_url = BASE + "/ws/device/live?device_id=" + DEVICE
    # Real device will use exactly this X-Device-Key header.
    async with connect(app_url, open_timeout=30) as phone1, connect(app_url, open_timeout=30) as phone2:
        initial1 = json.loads(await phone1.recv())
        initial2 = json.loads(await phone2.recv())
        print("[OK] App WS connected; initial radar:", initial1.get("radar"), initial2.get("radar"))
        async with connect(pi_url, additional_headers={"X-Device-Key": device_key}, open_timeout=30) as pi:
            print("[OK] Pi WS connected")
            await pi.send(json.dumps({
                "type": "frame",
                "t": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "radar": "ok",
                "people": [{
                    "id": "MOCK_T3", "x": 0.8, "y": 1.4,
                    "hidden": False, "situation": "배회",
                    "prob": 0.85, "abnormal": True,
                    "still_s": 2.0, "elapsed_s": 15.0,
                }],
            }, ensure_ascii=False))
            received1 = await until(phone1, lambda m: any(p.get("id") == "MOCK_T3" for p in m.get("people", [])))
            received2 = await until(phone2, lambda m: any(p.get("id") == "MOCK_T3" for p in m.get("people", [])))
            assert received1["people"][0]["x"] == 0.8 and received2["people"][0]["y"] == 1.4
            print("[OK] Frame relayed to both Flutter subscribers")

            await pi.send(json.dumps({
                "type": "status", "device_id": DEVICE,
                "radar": "ok", "frame_rate": 20.0,
                "camera": "ok", "camera_backend": "helper",
                "cpu_temp_c": 44.8,
                "storage": {"used_gb": 11.3, "total_gb": 60.9, "photos": 7},
            }))
            await asyncio.sleep(0.2)
            result = await asyncio.to_thread(status_from_http, app_token)
            assert result["camera"] == "ok", result
            print("[OK] GET /api/status updated from mock Pi")

            await pi.send(json.dumps({
                "type": "frame", "radar": "ok", "people": [],
                "t": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            }))
            await until(phone1, lambda m: m.get("radar") == "ok" and m.get("people") == [])
            print("[OK] people: [] relayed")
            # Stop sending and make sure stale clears people.
            await asyncio.sleep(3)
            await until(phone1, lambda m: m.get("radar") == "stale" and m.get("people") == [], timeout=8)
            print("[OK] stale state after frames stop")

        
        print("[TEST] Mock Pi socket closed")

        try:
            await until(
                phone1,
                lambda m: m.get("radar") == "disconnected",
                timeout=15,
            )
            print("[OK] Pi disconnected state relayed")
        except TimeoutError:
            print("[FAIL] No disconnected frame received")

            status = await asyncio.to_thread(
                status_from_http, app_token
            )
            print("[DEBUG] Cloud status radar:", status.get("radar"))
            print("[DEBUG] Status source:", status.get("status_source"))
            raise


    # Security smoke: unauthorized connection should be rejected.
    try:
        async with connect(pi_url, additional_headers={"X-Device-Key": "invalid-key"}, open_timeout=15):
            raise AssertionError("Bad Device Key unexpectedly accepted")
    except InvalidStatus as exc:
        assert exc.response.status_code == 403, "Unexpected status: " + str(exc.response.status_code)
        print("[OK] Incorrect Device Key rejected")
    print("M-LIVE-3 basic Cloud smoke PASSED")


if __name__ == "__main__":
    asyncio.run(main())
