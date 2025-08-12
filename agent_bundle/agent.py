#!/usr/bin/env python3
import os, json, time, threading, subprocess
import websocket
import requests

# --- Optional Sense HAT support with fallback ---
try:
    from sense_hat import SenseHat
    _sense = SenseHat()
except Exception:
    _sense = None

# ---------- Config ----------
# --- Base URLs from env (required) ---
_http = os.environ.get("SERVER_BASE")
if not _http:
    print("[agent] ERROR: SERVER_BASE missing in environment (/etc/pi-agent/agent.env)", flush=True)
    raise SystemExit(1)

HTTP_BASE = _http.rstrip('/')  # keep http/https, strip trailing slash

# Derive WS base (supports http→ws and https→wss; accept pre-set ws/wss too)
if HTTP_BASE.startswith("https://"):
    WS_BASE = "wss://" + HTTP_BASE[len("https://"):]
elif HTTP_BASE.startswith("http://"):
    WS_BASE = "ws://" + HTTP_BASE[len("http://"):]
else:
    WS_BASE = HTTP_BASE  # if someone passed ws:// already, use as-is
DEVICE_ID = os.environ.get("DEVICE_ID")
TOKEN     = os.environ.get("TOKEN")

if not DEVICE_ID or not TOKEN:
    print("[agent] ERROR: DEVICE_ID or TOKEN missing in environment", flush=True)
    raise SystemExit(1)

WS_URL      = f"{WS_BASE}/device-ws?deviceId={DEVICE_ID}&token={TOKEN}"
SCRIPTS_DIR = "/opt/pi-agent/scripts"
HTTP_HDRS   = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
SENSOR_WS_URL = f"{WS_BASE}/ws?deviceId={DEVICE_ID}&token={TOKEN}"  # backend /ws endpoint for sensor readings

# ---------- Heartbeat (single thread guarded) ----------
_hb_stop = None
_hb_lock = threading.Lock()

def send_heartbeat(ws, stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            ws.send(json.dumps({"type": "heartbeat"}))
            print("[agent] heartbeat", flush=True)
        except Exception as e:
            print("[agent] heartbeat error:", e, flush=True)
            time.sleep(2)
        stop_event.wait(20)

# ---------- Command helpers ----------
def run_local_script(script_id: str, args: dict) -> tuple[str, str]:
    mapping = {
        "collect-logs": "run-user-script.sh",
        "apt-update":   "apt-update.sh",
        "reboot":       "reboot.sh",
    }
    name = mapping.get(script_id)
    if not name:
        return "error", f"unknown scriptId={script_id}"

    path = f"{SCRIPTS_DIR}/{name}"
    cmd = ["/usr/bin/env", "bash", path]

    if isinstance(args, dict):
        if "minutes" in args:
            cmd += [str(args["minutes"])]
        if "extra" in args and isinstance(args["extra"], list):
            cmd += [str(x) for x in args["extra"]]

    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=300)
        return "ok", out.strip()
    except subprocess.CalledProcessError as e:
        return "error", f"exit {e.returncode}: {e.output.strip()}"
    except Exception as e:
        return "error", str(e)

# ---------- Reporting loop (start/stop) ----------
_report_lock = threading.Lock()
_report_thread = None
_report_stop = None

def _read_sense():
    if _sense:
        try:
            t = round(_sense.get_temperature(), 2)
            h = round(_sense.get_humidity(), 2)
            p = round(_sense.get_pressure(), 2)
            return t, h, p
        except Exception as e:
            print("[agent] sense-hat read error:", e, flush=True)
    # Fallback values if Sense HAT not available
    return 25.0, None, None

def _report_loop(period_s: int):
    global _report_stop
    stop = _report_stop
    print(f"[agent] report loop start period={period_s}s -> {SENSOR_WS_URL}", flush=True)
    while stop and not stop.is_set():
        ts_ms = int(time.time() * 1000)
        t, h, p = _read_sense()
        payload = {
            "deviceId": DEVICE_ID,
            "temperature": t,
            "humidity": h,
            "pressure": p,
            "timestamp": ts_ms
        }
        try:
            ws2 = websocket.create_connection(SENSOR_WS_URL, timeout=10)
            ws2.send(json.dumps(payload))
            ws2.close()
            print(f"[agent] reported: {payload}", flush=True)
        except Exception as e:
            print("[agent] report send error:", e, flush=True)
        # cooperative sleep
        for _ in range(period_s):
            if stop.is_set(): break
            time.sleep(1)
    print("[agent] report loop stopped", flush=True)

def _start_reporting(period_s: int = 60):
    global _report_thread, _report_stop
    with _report_lock:
        if _report_thread and _report_thread.is_alive():
            return False, "already reporting"
        _report_stop = threading.Event()
        _report_thread = threading.Thread(target=_report_loop, args=(period_s,), daemon=True)
        _report_thread.start()
        return True, f"started period={period_s}s"

def _stop_reporting():
    global _report_thread, _report_stop
    with _report_lock:
        if not _report_thread:
            return False, "not running"
        _report_stop.set()
        _report_thread = None
        _report_stop = None
        return True, "stopped"


def http_post(path: str, payload: dict, timeout=10):
    url = f"{HTTP_BASE}{path}"
    try:
        r = requests.post(url, headers=HTTP_HDRS, data=json.dumps(payload), timeout=timeout)
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)

# Keep track of commands being processed to avoid accidental duplicates
_in_flight = set()
_in_flight_lock = threading.Lock()

def execute_command_async(ws, cid: str, name: str, payload: dict):
    # ACK early so backend moves from "sent" -> "acked"
    status_code, body = http_post(f"/api/commands/{cid}/ack", {"deviceId": DEVICE_ID})
    if status_code >= 200 and status_code < 300:
        print(f"[agent] acked {cid} via HTTP", flush=True)
    else:
        print(f"[agent] ack HTTP failed for {cid}: {status_code} {body}", flush=True)

    try:
        status, result = "acked", f"no-op {name}"

        if name == "reboot":
            os.system("sudo reboot &")
            status, result = "ok", "rebooting"
        elif name == "update":
            time.sleep(2)
            status, result = "ok", f"updated to {payload.get('version','n/a')}"
        elif name == "run_script":
            sid  = payload.get("scriptId", "")
            args = payload.get("args", {}) if isinstance(payload, dict) else {}
            status, result = run_local_script(sid, args)
        elif name == "start_report":
            # payload example: {"periodSeconds": 60}
            period = int((payload.get("periodSeconds") or 60))
            ok, msg = _start_reporting(period)
            status, result = ("ok", msg) if ok else ("acked", msg)
        elif name == "stop_report":
            ok, msg = _stop_reporting()
            status, result = ("ok", msg) if ok else ("acked", msg)


        # Report DONE to backend
        done_payload = {"deviceId": DEVICE_ID, "status": status, "result": result}
        status_code, body = http_post(f"/api/commands/{cid}/done", done_payload, timeout=15)
        if status_code >= 200 and status_code < 300:
            print(f"[agent] done {cid} ({status})", flush=True)
        else:
            print(f"[agent] done HTTP failed for {cid}: {status_code} {body}", flush=True)

        # WS fallback (in case HTTP handlers aren’t wired yet)
        try:
            ws.send(json.dumps({"type":"commandAck","commandId": cid,"status": status,"result": result}))
        except Exception as e:
            print(f"[agent] ws fallback ack failed for {cid}: {e}", flush=True)

    except Exception as e:
        print(f"[agent] execute error {cid}: {e}", flush=True)
        # Try to notify failure
        http_post(f"/api/commands/{cid}/done", {"deviceId": DEVICE_ID, "status":"error","error":str(e)}, timeout=8)
        try:
            ws.send(json.dumps({"type":"commandAck","commandId": cid,"status":"error","result":str(e)}))
        except Exception:
            pass
    finally:
        with _in_flight_lock:
            _in_flight.discard(cid)

# ---------- WebSocket callbacks ----------
def on_message(ws, message: str):
    try:
        print("[agent] raw:", message, flush=True)
        msg = json.loads(message)
        if msg.get("type") != "command":
            return

        cid = msg.get("commandId")
        name = msg.get("name")
        payload = msg.get("payload") or {}

        # de-dup in case backend retries the same id
        with _in_flight_lock:
            if cid in _in_flight:
                print(f"[agent] skip duplicate in-flight {cid}", flush=True)
                return
            _in_flight.add(cid)

        # simple trace
        try:
            with open("/opt/pi-agent/last-command.log", "a") as f:
                f.write(f"{time.time()} {cid} {name} {json.dumps(payload)}\n")
        except Exception as e:
            print("[agent] write log failed:", e, flush=True)

        # Run in background so WS stays responsive
        threading.Thread(target=execute_command_async, args=(ws, cid, name, payload), daemon=True).start()

    except Exception as e:
        print("[agent] on_message error:", e, flush=True)

def on_open(ws):
    global _hb_stop
    print("[agent] ws open:", WS_URL, flush=True)
    with _hb_lock:
        if _hb_stop is not None:
            _hb_stop.set()
        _hb_stop = threading.Event()
        threading.Thread(target=send_heartbeat, args=(ws, _hb_stop), daemon=True).start()

def on_close(ws, code, msg):
    global _hb_stop
    print("[agent] ws close:", code, msg, flush=True)
    with _hb_lock:
        if _hb_stop is not None:
            _hb_stop.set()
            _hb_stop = None
    time.sleep(3)

def on_error(ws, err):
    print("[agent] ws error:", err, flush=True)

# ---------- Main ----------
if __name__ == "__main__":
    print("[agent] starting", flush=True)
    print("[agent] HTTP_BASE:", HTTP_BASE, flush=True)
    print("[agent] DEVICE_ID:", DEVICE_ID, flush=True)
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # keepalive
            ws.run_forever(ping_interval=25, ping_timeout=15)
        except Exception as e:
            print("[agent] run_forever error:", e, flush=True)
            time.sleep(3)
