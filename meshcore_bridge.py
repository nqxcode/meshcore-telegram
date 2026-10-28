import asyncio
import hashlib
import json
import os
import queue
import threading
import time
from collections import OrderedDict
from concurrent.futures import TimeoutError

from meshcore import EventType, MeshCore

import bridge


CHANNEL = int(os.getenv("MESHCORE_CHANNEL", "1"))
CHANNEL_NAME = os.getenv("MESHCORE_CHANNEL_NAME", "#tulameshbridge")
CHANNELS = {0: "Public", CHANNEL: CHANNEL_NAME}
PUBLIC_SECRET = bytes.fromhex("8b3387e9c5cdea6ac9e5edbaa115cd72")


def require_ok(result):
    if result is None or result.type != EventType.OK:
        raise RuntimeError(f"MeshCore не подтвердил команду: {getattr(result, 'type', None)}")


class MeshCoreRadio:
    def __init__(self, channel=CHANNEL):
        if CHANNEL == 0 or channel not in CHANNELS:
            raise ValueError("Неверный индекс канала MeshCore")

        self.channel = channel
        self.isConnected = threading.Event()
        self.nodes = {}
        self.seen = OrderedDict()
        self.outgoing = OrderedDict()
        self.name = ""
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

        try:
            self.call(self.connect(), timeout=60)
        except BaseException:
            self.close()
            raise

    def call(self, coroutine, timeout=30):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        try:
            return future.result(timeout)
        except TimeoutError:
            future.cancel()
            raise RuntimeError("Тайм-аут MeshCore; результат команды неизвестен") from None

    def sendText(self, text, channelIndex):
        if channelIndex != self.channel or not self.isConnected.is_set():
            raise RuntimeError("MeshCore отключён или выбран неверный канал")

        self.call(self.send(text))

    def close(self):
        try:
            if getattr(self, "radio", None) is not None and self.loop.is_running():
                self.call(self.radio.disconnect(), timeout=10)
        finally:
            self.isConnected.clear()
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=10)

    async def connect(self):
        self.radio = await MeshCore.create_serial(bridge.PORT, default_timeout=10)
        if not self.radio or not self.radio.is_connected or not self.radio.self_info:
            raise RuntimeError("Нет ответа USB Companion MeshCore")

        for channel in CHANNELS:
            await self.validate_channel(channel)

        self.name = self.radio.self_info.get("name", "")
        require_ok(await self.radio.commands.set_time(int(time.time())))
        await self.refresh_contacts()
        self.radio.subscribe(EventType.CHANNEL_MSG_RECV, self.on_message)
        self.radio.subscribe(EventType.DISCONNECTED, self.on_disconnect)
        await self.radio.start_auto_message_fetching()
        self.isConnected.set()

    async def validate_channel(self, channel):
        result = await self.radio.commands.get_channel(channel)
        name = CHANNELS[channel]
        expected_secret = PUBLIC_SECRET if channel == 0 else hashlib.sha256(name.encode()).digest()[:16]
        if result is None or result.type != EventType.CHANNEL_INFO or result.payload.get("channel_name") != name:
            raise RuntimeError("На устройстве не настроен канал Telegram-моста")

        secret = result.payload.get("channel_secret")
        if isinstance(secret, str):
            secret = bytes.fromhex(secret)
        if secret != expected_secret:
            raise RuntimeError("Ключ канала MeshCore не соответствует его имени")

    async def select_channel(self, channel, state):
        if channel not in CHANNELS or not self.isConnected.is_set():
            raise RuntimeError("Канал недоступен или MeshCore отключён")

        await self.validate_channel(channel)
        updated = dict(state, meshcore_channel=channel)
        # Сначала сохраняем выбор: ошибка диска не должна менять текущую маршрутизацию.
        bridge.save_state(updated)
        state.update(updated)
        self.channel = channel
        bridge.CHANNEL = channel

        # Очередь предыдущего канала не переносим в новый режим пересылки.
        while True:
            try:
                bridge.messages.get_nowait()
            except queue.Empty:
                break

        bridge.log.info("MeshCore channel selected: %s (%s)", channel, CHANNELS[channel])

    async def refresh_contacts(self):
        result = await self.radio.commands.get_contacts()
        if result.type != EventType.CONTACTS:
            raise RuntimeError("Не удалось прочитать контакты MeshCore")

        self.nodes = {
            key: {"user": {"longName": contact.get("adv_name", "Без имени"), "shortName": key[:12]}}
            for key, contact in result.payload.items()
        }

    async def send(self, text):
        now = time.monotonic()
        self.prune(self.outgoing, now)
        self.outgoing[(self.channel, f"{self.name}: {text}")] = now
        require_ok(await self.radio.commands.send_chan_msg(self.channel, text))

    async def on_disconnect(self, event):
        self.isConnected.clear()

    @staticmethod
    def prune(cache, now):
        while cache and next(iter(cache.values())) <= now - bridge.DEDUP_SECONDS:
            cache.popitem(last=False)

        while len(cache) >= 4096:
            cache.popitem(last=False)

    async def on_message(self, event):
        message = event.payload
        text = message.get("text", "")
        channel = message.get("channel_idx")
        if channel != self.channel or not text:
            return

        now = time.monotonic()
        self.prune(self.seen, now)
        self.prune(self.outgoing, now)
        identity = (channel, message.get("sender_timestamp"), text)
        if (channel, text) in self.outgoing or identity in self.seen:
            return

        self.seen[identity] = now
        try:
            bridge.messages.put_nowait((channel, text))
        except queue.Full:
            bridge.log.warning("Очередь MeshCore → Telegram заполнена; сообщение пропущено")


def status_text(interface):
    return (
        f"MeshCore USB: {'подключён' if interface.isConnected.is_set() else 'отключён'}\n"
        f"Канал: {CHANNELS[interface.channel]}\n"
        f"Время работы моста: {bridge.duration(time.monotonic() - bridge.STARTED_AT)}\n"
        f"Известных контактов: {len(interface.nodes)}; доступность не проверена"
    )


def handle_message(interface, message, state, status_provider=status_text):
    if message.get("chat", {}).get("type") == "private":
        bridge.handle_message(interface, message, state, status_provider)
        return

    if message.get("chat", {}).get("id") != bridge.CHAT_ID or message.get("from", {}).get("is_bot"):
        return

    parsed = bridge.parse_message(message)
    if parsed is None:
        return

    command, parts = parsed
    if command not in ("/channels", "/channel"):
        bridge.handle_message(interface, message, state, status_provider)
        return

    handle_channel_command(interface, message, state, command, parts)


def handle_channel_command(interface, message, state, command, parts):
    respond = lambda text: bridge.reply_to(message, text)
    if command == "/channels" or len(parts) == 1:
        respond(
            f"Текущий канал: {CHANNELS[interface.channel]}\n"
            "/channel public — общий Public\n"
            "/channel bridge — #tulameshbridge\n"
            "Выбор общий для всей Telegram-группы и меняет пересылку в обе стороны."
        )
        return

    aliases = {"public": 0, "0": 0, "bridge": CHANNEL, str(CHANNEL): CHANNEL, CHANNEL_NAME.lower(): CHANNEL}
    channel = aliases.get(parts[1].strip().lower())
    if channel is None:
        respond("Неизвестный канал. Используйте /channel public или /channel bridge.")
        return

    try:
        interface.call(interface.select_channel(channel, state))
    except Exception:
        bridge.log.exception("Не удалось переключить канал MeshCore")
        respond("Не удалось переключить канал. Проверьте текущий выбор командой /channel.")
        return

    respond(
        f"Выбран канал {CHANNELS[channel]}. Пересылка в обе стороны переключена.\n"
        + ("Сообщения группы теперь уходят в общий радиоэфир Public." if channel == 0 else "Сообщения группы идут в #tulameshbridge.")
    )


def load_state():
    if bridge.STATE_PATH.exists():
        state = json.loads(bridge.STATE_PATH.read_text(encoding="utf-8"))
    else:
        updates = bridge.telegram("getUpdates", offset=-1, timeout=0, allowed_updates=["message"])
        state = {"offset": updates[-1]["update_id"] + 1 if updates else 0}
        bridge.save_state(state)

    if state.get("meshcore_channel", CHANNEL) not in CHANNELS:
        raise ValueError("В файле состояния указан неизвестный канал")

    return state


def main():
    # Оставляем запас для имени узла, которое прошивка добавляет в канальное сообщение.
    bridge.MAX_MESSAGE_BYTES = 120
    bridge.bot_username = bridge.telegram("getMe")["username"]
    state = load_state()
    bridge.CHANNEL = state.get("meshcore_channel", CHANNEL)
    radio = MeshCoreRadio(bridge.CHANNEL)

    try:
        bridge.log.info("MeshCore bridge started: %s, channel %s (%s)", bridge.PORT, radio.channel, CHANNELS[radio.channel])
        refreshed = time.monotonic()
        while radio.isConnected.is_set():
            while not bridge.messages.empty():
                channel, text = bridge.messages.get_nowait()
                if channel != radio.channel:
                    continue

                try:
                    bridge.reply(f"[MeshCore {CHANNELS[channel]}] {text}")
                    bridge.log.info("MeshCore -> TG: %s", text)
                except Exception:
                    bridge.log.exception("Ошибка отправки в Telegram")

            if time.monotonic() - refreshed > 300:
                radio.call(radio.refresh_contacts())
                refreshed = time.monotonic()

            try:
                bridge.receive_telegram(radio, state, status_provider=status_text, message_handler=handle_message)
            except Exception:
                bridge.log.exception("Ошибка опроса Telegram")
                time.sleep(5)

        raise RuntimeError("USB-соединение MeshCore потеряно")
    finally:
        radio.close()


if __name__ == "__main__":
    main()
