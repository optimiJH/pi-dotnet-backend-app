#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/etc/pi-agent/agent.env"
INSTALL_DIR="/opt/pi-agent"
UNIT_NAME="device-agent.service"

sudo mkdir -p /etc/pi-agent "$INSTALL_DIR"
sudo cp -r . "$INSTALL_DIR"
sudo chown -R root:root "$INSTALL_DIR"
sudo chmod +x "$INSTALL_DIR/install.sh" || true
sudo chmod +x "$INSTALL_DIR"/scripts/*.sh || true

# Python deps (for agent + JSON parsing)
if ! command -v python3 >/dev/null 2>&1; then
  sudo apt-get update -y && sudo apt-get install -y python3 python3-pip
fi
# The websocket client is for agent.py
sudo python3 -m pip install --break-system-packages websocket-client || sudo python3 -m pip install websocket-client

# ---------- Load and validate env ----------
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Write SERVER_BASE and ENROLLMENT_KEY there before running install.sh."
  exit 1
fi

set -a
# shellcheck source=/etc/pi-agent/agent.env
source "$ENV_FILE"
set +a

if [[ -z "${SERVER_BASE:-}" ]]; then
  echo "ERROR: SERVER_BASE is empty in $ENV_FILE"
  exit 1
fi
if [[ -z "${ENROLLMENT_KEY:-}" ]]; then
  echo "ERROR: ENROLLMENT_KEY is empty in $ENV_FILE (create a new invite and set it)."
  exit 1
fi
if [[ "$SERVER_BASE" == *"localhost"* ]]; then
  echo "ERROR: SERVER_BASE uses 'localhost'; from the Pi this points to itself. Use your server's LAN IP, e.g. http://192.168.x.x:5005"
  exit 1
fi

# ---------- Preflight: verify backend is reachable ----------
HTTP_HEALTH=$(curl -sS -o /dev/null -w "%{http_code}" "$SERVER_BASE/api/agent/version" || true)
if [[ "$HTTP_HEALTH" != "200" ]]; then
  HTTP_HEALTH=$(curl -sS -o /dev/null -w "%{http_code}" "$SERVER_BASE/" || true)
fi
if [[ "$HTTP_HEALTH" != "200" ]]; then
  echo "ERROR: Cannot reach $SERVER_BASE (HTTP $HTTP_HEALTH). Check IP/port and firewall."
  exit 1
fi

# ---------- Enroll and capture response safely ----------
payload=$(printf '{"deviceFingerprint":"%s","enrollmentKey":"%s"}' "$(hostname)" "$ENROLLMENT_KEY")
ENROLL_JSON="/tmp/enroll.json"
HTTP_CODE=$(curl -sS -o "$ENROLL_JSON" -w "%{http_code}" \
  -H "Content-Type: application/json" \
  -d "$payload" \
  "$SERVER_BASE/api/devices/enroll" || true)

echo "Enroll HTTP $HTTP_CODE"
echo "Enroll response:"
cat "$ENROLL_JSON" || true
echo

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: Enroll failed (HTTP $HTTP_CODE). Check SERVER_BASE reachability and that ENROLLMENT_KEY is fresh."
  exit 1
fi

# Parse JSON using python (avoid jq dependency)
DEVICE_ID=$(python3 -c 'import sys, json; print(json.load(open(sys.argv[1]))["deviceId"])' "$ENROLL_JSON")
TOKEN=$(python3 -c 'import sys, json; print(json.load(open(sys.argv[1]))["token"])' "$ENROLL_JSON")

if [[ -z "$DEVICE_ID" || -z "$TOKEN" ]]; then
  echo "ERROR: Could not parse deviceId/token from enroll response."
  exit 1
fi

# ---------- Persist final env ----------
sudo bash -c "cat > '$ENV_FILE'" <<EOF
SERVER_BASE=$SERVER_BASE
DEVICE_ID=$DEVICE_ID
TOKEN=$TOKEN
ENROLLMENT_KEY=
EOF

# ---------- Install and start systemd unit ----------
sudo cp "$INSTALL_DIR/$UNIT_NAME" "/etc/systemd/system/$UNIT_NAME"

# Ensure systemd reads env and uses unbuffered Python; keep restart policy
sudo mkdir -p "/etc/systemd/system/$UNIT_NAME.d"
sudo bash -c "cat > /etc/systemd/system/$UNIT_NAME.d/override.conf" <<'EOF'
[Service]
EnvironmentFile=/etc/pi-agent/agent.env
Environment=PYTHONUNBUFFERED=1
Restart=always
RestartSec=3
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "$UNIT_NAME"

echo "Agent installed and started (deviceId=$DEVICE_ID)."
