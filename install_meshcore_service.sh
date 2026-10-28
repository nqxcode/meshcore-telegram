#!/usr/bin/env bash
set -euo pipefail

# Сохраняем прежний сервис и его окружение; меняем только исполняемый файл.
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
install -d /etc/systemd/system/meshtastic-telegram.service.d
install -m 644 "$project_dir/meshcore-systemd.conf" /etc/systemd/system/meshtastic-telegram.service.d/meshcore.conf
systemctl daemon-reload
systemctl restart meshtastic-telegram.service
systemctl is-active meshtastic-telegram.service
