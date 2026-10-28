import hashlib
import json
import logging
import os
import queue
import time
from collections import OrderedDict
from functools import partial
from pathlib import Path

import requests

PORT = os.getenv("MESH_PORT", "/dev/ttyACM0")
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
CHANNEL = int(os.getenv("MESH_CHANNEL", "0"))
SEND_INTERVAL = max(1, int(os.getenv("MESH_SEND_INTERVAL", "1")))
STATE_PATH = Path(os.getenv("BRIDGE_STATE_FILE", str(Path(__file__).with_name("bridge-state.json"))))
STARTED_AT = time.monotonic()
DEDUP_SECONDS = 300
OUTGOING_DEDUP_SECONDS = 1
MAX_MESSAGE_BYTES = 180

API = f"https://api.telegram.org/bot{TOKEN}"
messages = queue.Queue(maxsize=200)
seen_packets = OrderedDict()
bot_username = ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("bridge")

session = requests.Session()


def telegram(method, **data):
    response = session.post(
        f"{API}/{method}",
        json=data,
        timeout=35,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        raise RuntimeError(result)
    return result["result"]


def on_receive(packet, interface):
    log.info("RX packet: %s", packet)

    decoded = packet.get("decoded", {})
    text = decoded.get("text")

    if not text:
        return

    if packet.get("channel", 0) != CHANNEL:
        return

    # Исключаем собственные сообщения шлюза
    if packet.get("from") == getattr(interface.myInfo, "my_node_num", None):
        return

    now = time.monotonic()
    while seen_packets and next(iter(seen_packets.values())) <= now - DEDUP_SECONDS:
        seen_packets.popitem(last=False)

    packet_id = (packet.get("from"), packet.get("id"))
    if packet.get("id") is not None:
        if packet_id in seen_packets:
            return

        seen_packets[packet_id] = now
        if len(seen_packets) > 4096:
            seen_packets.popitem(last=False)

    sender = packet.get("fromId", "unknown")
    try:
        messages.put_nowait((sender, text))
    except queue.Full:
        log.warning("Очередь входящих сообщений заполнена; пакет пропущен")


def save_state(state):
    temporary = STATE_PATH.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(state, stream)
        stream.flush()
        os.fsync(stream.fileno())

    temporary.replace(STATE_PATH)


def reply(text, chat_id=CHAT_ID):
    # Ограничиваем размер ответа с учётом Unicode и лимита Telegram.
    for start in range(0, len(text), 2000):
        telegram("sendMessage", chat_id=chat_id, text=text[start:start + 2000])


def reply_to(message, text):
    sender = message.get("from", {})
    user_id = sender.get("id")
    if not user_id or sender.get("is_bot") or message.get("sender_chat"):
        return

    reply(text, chat_id=user_id)


def duration(seconds):
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{days} д {hours:02}:{minutes:02}:{seconds:02}"


def status_text(interface):
    nodes = dict(interface.nodes or {})
    local = next((node for node in nodes.values()
                  if node.get("num") == getattr(interface.myInfo, "my_node_num", None)), {})
    uptime = local.get("deviceMetrics", {}).get("uptimeSeconds")
    return (
        f"Heltec: {'подключён' if interface.isConnected.is_set() else 'отключён'}\n"
        f"Порт: {PORT}; канал: {CHANNEL}\n"
        f"Время работы моста: {duration(time.monotonic() - STARTED_AT)}\n"
        f"Время работы Heltec (последняя телеметрия): {duration(uptime) if uptime is not None else 'нет данных'}\n"
        f"Известных узлов: {len(nodes)}"
    )


def node_list_pages(nodes):
    if not nodes:
        return ["📡 Известные узлы: 0\n\nСписок пока пуст."]

    entries = []
    for node_id, node in nodes.items():
        user = node.get("user", {})
        name = " ".join(str(user.get("longName") or "Без имени").split()) or "Без имени"
        short_name = " ".join(str(user.get("shortName") or "—").split()) or "—"
        identifier = str(node_id)
        entries.append((name, identifier, short_name))

    entries.sort(key=lambda entry: (entry[0].casefold(), entry[1]))
    groups = []
    current = []
    current_size = 0
    for index, (name, identifier, short_name) in enumerate(entries, start=1):
        entry = f"{index}. {name}\nКороткое имя: {short_name}\nID: {identifier}"
        entry_size = len(entry.encode("utf-16-le")) // 2 + 2
        if current and (len(current) == 10 or current_size + entry_size > 1800):
            groups.append(current)
            current = []
            current_size = 0

        current.append(entry)
        current_size += entry_size

    if current:
        groups.append(current)

    pages = []
    for page_number, group in enumerate(groups, start=1):
        heading = f"📡 Известные узлы: {len(entries)}"
        if len(groups) > 1:
            heading += f" · {page_number}/{len(groups)}"

        pages.append(heading + "\nДоступность узлов сейчас не проверена.\n\n" + "\n\n".join(group))

    return pages


def handle_message(interface, message, state, status_provider=status_text):
    respond = partial(reply_to, message)
    sender = message.get("from", {})
    if sender.get("is_bot"):
        return

    text = message.get("text", "")
    parts = text.split(maxsplit=1)
    if not parts:
        return

    command, _, target = parts[0].partition("@")
    if command.startswith("/") and target and target.lower() != bot_username.lower():
        return

    chat = message.get("chat", {})
    if chat.get("id") != CHAT_ID:
        if chat.get("type") != "private" or command not in ("/status", "/nodes"):
            return

    if command == "/status":
        reply(status_provider(interface), chat_id=chat["id"])
        return

    if command == "/nodes":
        for page in node_list_pages(dict(interface.nodes or {})):
            reply(page, chat_id=chat["id"])
        return

    if command == "/send":
        if len(parts) != 2 or not parts[1].strip():
            respond("Использование: /send текст")
            return

        body = parts[1].strip()
    else:
        body = text.strip()

    payload = f"[TG {sender.get('first_name', 'Telegram')}] {body}"
    if len(payload.encode("utf-8")) > MAX_MESSAGE_BYTES:
        respond(f"Сообщение слишком длинное: максимум {MAX_MESSAGE_BYTES} байт UTF-8 вместе с префиксом отправителя.")
        return

    if not interface.isConnected.is_set():
        respond("Heltec отключён. Сообщение не отправлено.")
        return

    now = time.time()
    recent = {key: timestamp for key, timestamp in state.get("recent", {}).items()
              if now - timestamp < OUTGOING_DEDUP_SECONDS}
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    if digest in recent:
        respond("Этот текст уже отправлялся за последнюю секунду.")
        return

    remaining = SEND_INTERVAL - (now - state.get("last_send", 0))
    if remaining > 0:
        respond(f"Ограничение частоты: повторите через {int(remaining) + 1} с.")
        return

    # Сохраняем попытку до обращения к радио: при неясном результате не повторяем её автоматически.
    recent[digest] = now
    state.update(last_send=now, recent=recent)
    save_state(state)

    try:
        interface.sendText(payload, channelIndex=CHANNEL)
    except Exception:
        log.exception("Ошибка отправки в LoRa")
        respond("Ошибка отправки: результат неизвестен. Автоматического повтора не будет.")
        return

    log.info("TG -> LoRa: %s", payload)
    respond(f"Передано в mesh-сеть:\n{payload}")


def receive_telegram(interface, state, status_provider=status_text, message_handler=handle_message):
    result = telegram(
        "getUpdates",
        offset=state["offset"],
        timeout=20,
        allowed_updates=["message"],
    )

    for update in result:
        if update["update_id"] < state["offset"]:
            continue

        state["offset"] = update["update_id"] + 1
        save_state(state)
        try:
            message_handler(interface, update.get("message", {}), state, status_provider)
        except Exception:
            log.exception("Ошибка обработки Telegram-команды")


def main():
    import meshtastic.serial_interface
    from pubsub import pub

    global bot_username

    bot_username = telegram("getMe")["username"]
    pub.subscribe(on_receive, "meshtastic.receive")

    log.info("Connecting to %s", PORT)
    interface = meshtastic.serial_interface.SerialInterface(
        devPath=PORT
    )

    try:
        if STATE_PATH.exists():
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        else:
            # При первом запуске пропускаем всю старую очередь, включая очереди длиннее 100 сообщений.
            updates = telegram("getUpdates", offset=-1, timeout=0, allowed_updates=["message"])
            state = {"offset": updates[-1]["update_id"] + 1 if updates else 0}
            save_state(state)

        log.info("Bridge started on channel %s", CHANNEL)

        while True:
            # LoRa -> Telegram
            while not messages.empty():
                sender, text = messages.get()
                try:
                    telegram(
                        "sendMessage",
                        chat_id=CHAT_ID,
                        text=f"[LoRa {sender}] {text}",
                    )
                    log.info("LoRa -> TG: %s", text)
                except Exception:
                    log.exception("Telegram send failed")

            # Telegram -> LoRa
            try:
                receive_telegram(interface, state)
            except Exception:
                log.exception("Polling failed")
                time.sleep(5)

    finally:
        interface.close()


if __name__ == "__main__":
    main()
