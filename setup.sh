#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# Rivian Gear Shop Crawler — Debian Setup Script
# Run as root: sudo bash setup.sh
# ────────────────────────────────────────────────────────────────
set -euo pipefail

INSTALL_DIR="/opt/rivian-gearshop-crawler"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    fonts-liberation libappindicator3-1 xdg-utils 2>/dev/null || true

echo "==> Creating install directory: ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

echo "==> Copying project files..."
cp "${REPO_DIR}/crawler.py" "${INSTALL_DIR}/"
cp "${REPO_DIR}/availability.py" "${INSTALL_DIR}/"
cp "${REPO_DIR}/requirements.txt" "${INSTALL_DIR}/"
cp "${REPO_DIR}/.env" "${INSTALL_DIR}/"
chmod 600 "${INSTALL_DIR}/.env"

echo "==> Creating Python virtual environment..."
python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
"${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q

echo "==> Installing Playwright Chromium browser..."
"${INSTALL_DIR}/venv/bin/python3" -m playwright install chromium
"${INSTALL_DIR}/venv/bin/python3" -m playwright install-deps chromium 2>/dev/null || true

echo "==> Installing systemd service and timer..."
cp "${REPO_DIR}/rivian-gearshop-crawler.service" /etc/systemd/system/
cp "${REPO_DIR}/rivian-gearshop-crawler.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable rivian-gearshop-crawler.timer
systemctl start rivian-gearshop-crawler.timer

echo ""
echo "=========================================="
echo "  Setup complete!"
echo "=========================================="
echo ""
echo "  Install dir:  ${INSTALL_DIR}"
echo "  Timer:        every 60 minutes"
echo "  Config:       ${INSTALL_DIR}/.env"
echo ""
echo "  Useful commands:"
echo "    systemctl status rivian-gearshop-crawler.timer   # check timer"
echo "    systemctl list-timers --all                      # list all timers"
echo "    journalctl -u rivian-gearshop-crawler -f         # follow logs"
echo "    systemctl start rivian-gearshop-crawler.service  # run manually now"
echo ""
