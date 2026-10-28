#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Экранируем путь для кавычек, спецификаторов и переменных в ExecStart systemd.
systemd_project_dir="${project_dir//\\/\\\\}"
systemd_project_dir="${systemd_project_dir//\"/\\\"}"
systemd_project_dir="${systemd_project_dir//%/%%}"
systemd_project_dir="${systemd_project_dir//\$/\$\$}"

service_config="$(< "$project_dir/meshcore-systemd.conf")"
shopt -u patsub_replacement
service_config="${service_config//@PROJECT_DIR@/$systemd_project_dir}"

rendered_config="$(mktemp)"
trap 'rm -f -- "$rendered_config"' EXIT
printf '%s\n' "$service_config" > "$rendered_config"

# Сохраняем прежний сервис и его окружение; меняем только исполняемый файл.
install -d /etc/systemd/system/meshtastic-telegram.service.d
install -m 644 "$rendered_config" /etc/systemd/system/meshtastic-telegram.service.d/meshcore.conf
systemctl daemon-reload
systemctl restart meshtastic-telegram.service
systemctl is-active meshtastic-telegram.service
