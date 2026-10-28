import asyncio
import json
import os
import queue
import tempfile
import threading
import unittest
from collections import OrderedDict
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

with patch.dict(os.environ, TELEGRAM_BOT_TOKEN="test", TELEGRAM_CHAT_ID="123"):
    import meshcore_bridge as gateway


class MeshCoreTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.radio = object.__new__(gateway.MeshCoreRadio)
        self.radio.seen = OrderedDict()
        self.radio.outgoing = OrderedDict()
        self.radio.name = "TulaMeshBridge"
        self.radio.channel = gateway.CHANNEL
        self.radio.radio = SimpleNamespace(commands=SimpleNamespace(send_chan_msg=AsyncMock()))
        self.messages = queue.Queue(maxsize=200)
        patcher = patch.object(gateway.bridge, "messages", self.messages)
        patcher.start()
        self.addCleanup(patcher.stop)

    def event(self, text="Иван: привет", channel=gateway.CHANNEL, timestamp=100):
        return SimpleNamespace(payload={
            "text": text,
            "channel_idx": channel,
            "sender_timestamp": timestamp,
        })

    async def test_only_selected_channel_and_no_duplicates(self):
        await self.radio.on_message(self.event(channel=0))
        await self.radio.on_message(self.event())
        await self.radio.on_message(self.event())
        await self.radio.on_message(self.event(timestamp=101))

        self.assertEqual(self.messages.qsize(), 2)
        self.assertEqual(self.messages.get_nowait(), (gateway.CHANNEL, "Иван: привет"))

    async def test_radio_rejection_is_an_error(self):
        self.radio.radio.commands.send_chan_msg.return_value = SimpleNamespace(type=gateway.EventType.ERROR)

        with self.assertRaises(RuntimeError):
            await self.radio.send("Привет")

        self.radio.radio.commands.send_chan_msg.assert_awaited_once_with(gateway.CHANNEL, "Привет")

    async def test_own_echo_is_not_sent_back_to_telegram(self):
        self.radio.radio.commands.send_chan_msg.return_value = SimpleNamespace(type=gateway.EventType.OK)
        await self.radio.send("[TG Андрей] Привет")
        await self.radio.on_message(self.event("TulaMeshBridge: [TG Андрей] Привет"))

        self.assertTrue(self.messages.empty())

    async def test_expired_duplicate_is_allowed(self):
        with patch.object(gateway.time, "monotonic", return_value=1000):
            await self.radio.on_message(self.event())

        with patch.object(gateway.time, "monotonic", return_value=1301):
            await self.radio.on_message(self.event())

        self.assertEqual(self.messages.qsize(), 2)

    async def test_public_receive_and_send_use_selected_channel(self):
        self.radio.channel = 0
        self.radio.radio.commands.send_chan_msg.return_value = SimpleNamespace(type=gateway.EventType.OK)

        await self.radio.on_message(self.event(channel=gateway.CHANNEL))
        await self.radio.on_message(self.event(channel=0))
        await self.radio.send("Привет")

        self.assertEqual(self.messages.qsize(), 1)
        self.assertEqual(self.messages.get_nowait(), (0, "Иван: привет"))
        self.radio.radio.commands.send_chan_msg.assert_awaited_once_with(0, "Привет")

    async def test_validate_channel_rejects_wrong_public_key(self):
        self.radio.radio.commands.get_channel = AsyncMock(return_value=SimpleNamespace(
            type=gateway.EventType.CHANNEL_INFO,
            payload={"channel_name": "Public", "channel_secret": bytes(16)},
        ))

        with self.assertRaises(RuntimeError):
            await self.radio.validate_channel(0)


    async def test_channel_metadata_validation(self):
        cases = [
            None,
            SimpleNamespace(type=gateway.EventType.ERROR, payload={}),
            SimpleNamespace(type=gateway.EventType.CHANNEL_INFO, payload={"channel_name": "Другой"}),
        ]
        self.radio.radio.commands.get_channel = AsyncMock()
        for result in cases:
            with self.subTest(result=result):
                self.radio.radio.commands.get_channel.return_value = result
                with self.assertRaisesRegex(RuntimeError, "не настроен"):
                    await self.radio.validate_channel(0)

    async def test_select_channel_rejects_disconnected_or_unknown_channel(self):
        self.radio.isConnected = threading.Event()
        for connected, channel in ((False, 0), (True, 999)):
            with self.subTest(connected=connected, channel=channel):
                self.radio.isConnected.set() if connected else self.radio.isConnected.clear()
                with self.assertRaisesRegex(RuntimeError, "недоступен"):
                    await self.radio.select_channel(channel, {"offset": 1})

        self.assertEqual(self.radio.channel, gateway.CHANNEL)

    async def test_contacts_error_does_not_replace_existing_nodes(self):
        self.radio.nodes = {"old": {"user": {"longName": "Старый"}}}
        self.radio.radio.commands.get_contacts = AsyncMock(return_value=SimpleNamespace(type=gateway.EventType.ERROR))

        with self.assertRaisesRegex(RuntimeError, "контакты"):
            await self.radio.refresh_contacts()

        self.assertIn("old", self.radio.nodes)

    async def test_cache_expiry_boundary_keeps_newer_messages(self):
        with patch.object(gateway, "time", SimpleNamespace(monotonic=lambda: 1000)):
            await self.radio.on_message(self.event("Первое", timestamp=1))
        with patch.object(gateway, "time", SimpleNamespace(monotonic=lambda: 1001)):
            await self.radio.on_message(self.event("Второе", timestamp=2))
        with patch.object(gateway, "time", SimpleNamespace(monotonic=lambda: 1300)):
            await self.radio.on_message(self.event("Первое", timestamp=1))
            await self.radio.on_message(self.event("Второе", timestamp=2))

        self.assertEqual(self.messages.qsize(), 3)
        self.assertEqual(list(self.radio.seen), [
            (gateway.CHANNEL, 2, "Второе"),
            (gateway.CHANNEL, 1, "Первое"),
        ])

    async def test_cache_capacity_evicts_oldest(self):
        self.radio.seen.update(((gateway.CHANNEL, i, str(i)), 1000) for i in range(4096))
        with patch.object(gateway, "time", SimpleNamespace(monotonic=lambda: 1000)):
            await self.radio.on_message(self.event())

        self.assertEqual(len(self.radio.seen), 4096)
        self.assertNotIn((gateway.CHANNEL, 0, "0"), self.radio.seen)
        self.assertIn((gateway.CHANNEL, 1, "1"), self.radio.seen)
        self.assertIn((gateway.CHANNEL, 4095, "4095"), self.radio.seen)

    async def test_empty_message_and_full_queue(self):
        await self.radio.on_message(self.event(""))
        self.assertTrue(self.messages.empty())
        for i in range(200):
            self.messages.put_nowait((gateway.CHANNEL, str(i)))

        await self.radio.on_message(self.event("Переполнение"))

        self.assertEqual(self.messages.qsize(), 200)
        self.assertEqual(self.messages.get_nowait(), (gateway.CHANNEL, "0"))


class ChannelCommandTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "state.json"
        self.messages = queue.Queue()
        self.reply = Mock()
        for name, value in {
            "STATE_PATH": self.path,
            "messages": self.messages,
            "reply": self.reply,
            "CHAT_ID": 123,
            "bot_username": "mesh_bot",
            "CHANNEL": gateway.CHANNEL,
        }.items():
            patcher = patch.object(gateway.bridge, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.radio = object.__new__(gateway.MeshCoreRadio)
        self.radio.channel = gateway.CHANNEL
        self.radio.isConnected = threading.Event()
        self.radio.isConnected.set()
        self.radio.validate_channel = AsyncMock()
        self.radio.call = asyncio.run
        self.state = {"offset": 17}

    def message(self, text, chat=123, bot=False):
        return {
            "chat": {"id": chat},
            "from": {"id": 42, "is_bot": bot},
            "text": text,
        }

    def test_switch_persists_and_clears_previous_queue(self):
        self.messages.put((gateway.CHANNEL, "Старое сообщение"))

        gateway.handle_message(self.radio, self.message("/channel@mesh_bot public"), self.state)

        self.assertEqual(self.radio.channel, 0)
        self.assertEqual(gateway.bridge.CHANNEL, 0)
        self.assertEqual(gateway.load_state(), {"offset": 17, "meshcore_channel": 0})
        self.assertTrue(self.messages.empty())

        gateway.handle_message(self.radio, self.message("/channel #tulameshbridge"), self.state)

        self.assertEqual(self.radio.channel, gateway.CHANNEL)
        self.assertEqual(gateway.load_state()["meshcore_channel"], gateway.CHANNEL)

    def test_failed_save_keeps_channel_and_state(self):
        with patch.object(gateway.bridge, "save_state", side_effect=OSError("disk full")):
            gateway.handle_message(self.radio, self.message("/channel public"), self.state)

        self.assertEqual(self.radio.channel, gateway.CHANNEL)
        self.assertEqual(self.state, {"offset": 17})
        self.assertEqual(gateway.bridge.CHANNEL, gateway.CHANNEL)

    def test_failed_validation_keeps_channel(self):
        self.radio.validate_channel.side_effect = RuntimeError("Неверный ключ")

        gateway.handle_message(self.radio, self.message("/channel public"), self.state)

        self.assertEqual(self.radio.channel, gateway.CHANNEL)
        self.assertFalse(self.path.exists())

    def test_list_invalid_and_unauthorized_commands_do_not_switch_or_transmit(self):
        messages = [
            self.message("/channels"),
            self.message("/channel"),
            self.message("/channel invalid"),
            self.message("/channel public", chat=456),
            self.message("/channel public", bot=True),
            self.message("/channel@other_bot public"),
        ]
        with patch.object(gateway.bridge, "handle_message") as forward:
            for message in messages:
                gateway.handle_message(self.radio, message, self.state)

        forward.assert_not_called()
        self.radio.validate_channel.assert_not_awaited()
        self.assertEqual(self.radio.channel, gateway.CHANNEL)

    def test_channel_responses_go_to_sender(self):
        for text in ("/channels", "/channel invalid", "/channel public"):
            with self.subTest(text=text):
                self.reply.reset_mock()

                gateway.handle_message(self.radio, self.message(text), self.state)

                self.reply.assert_called_once()
                self.assertEqual(self.reply.call_args.kwargs["chat_id"], 42)

    def test_private_status_and_nodes_reply_only_to_sender(self):
        self.radio.nodes = {"node1": {"user": {"longName": "Сосед"}}}
        for command, expected in (("/status", "MeshCore USB"), ("/nodes", "Сосед"),
                                  ("/status@mesh_bot", "MeshCore USB"), ("/nodes@mesh_bot", "Сосед")):
            with self.subTest(command=command):
                self.reply.reset_mock()
                message = self.message(command)
                message["chat"] = {"id": 42, "type": "private"}

                gateway.handle_message(self.radio, message, self.state)

                self.reply.assert_called_once()
                self.assertEqual(self.reply.call_args.kwargs["chat_id"], 42)
                self.assertIn(expected, self.reply.call_args.args[0])

        self.radio.validate_channel.assert_not_awaited()
        self.assertEqual(self.state, {"offset": 17})

    def test_private_radio_commands_and_other_groups_are_ignored(self):
        cases = [
            ("/send Привет", "private", False),
            ("Привет", "private", False),
            ("/channel public", "private", False),
            ("/channels", "private", False),
            ("/status@other_bot", "private", False),
            ("/status", "private", True),
            ("/status", "group", False),
            ("/nodes", "supergroup", False),
        ]
        for command, chat_type, bot in cases:
            with self.subTest(command=command, chat_type=chat_type, bot=bot):
                message = self.message(command, bot=bot)
                message["chat"] = {"id": 42, "type": chat_type}

                gateway.handle_message(self.radio, message, self.state)

        self.reply.assert_not_called()
        self.radio.validate_channel.assert_not_awaited()
        self.assertEqual(self.state, {"offset": 17})

    def test_selected_channel_applies_to_next_message_in_same_poll(self):
        updates = [
            {"update_id": 17, "message": self.message("/channel public")},
            {"update_id": 18, "message": self.message("Привет")},
        ]
        selected = []
        with patch.object(gateway.bridge, "telegram", return_value=updates):
            with patch.object(gateway.bridge, "handle_message", side_effect=lambda *args: selected.append(args[0].channel)):
                gateway.bridge.receive_telegram(self.radio, self.state, message_handler=gateway.handle_message)

        self.assertEqual(selected, [0])
        self.assertEqual(json.loads(self.path.read_text()), {"offset": 19, "meshcore_channel": 0})


    def test_numeric_channel_aliases_and_feedback(self):
        for alias, channel in (("0", 0), ("1", gateway.CHANNEL), ("PUBLIC", 0), ("bridge", gateway.CHANNEL)):
            with self.subTest(alias=alias):
                self.reply.reset_mock()

                gateway.handle_message(self.radio, self.message("/channel " + alias), self.state)

                self.assertEqual(self.radio.channel, channel)
                text = self.reply.call_args.args[0]
                self.assertIn("Выбран канал " + gateway.CHANNELS[channel], text)
                self.assertIn("общий радиоэфир Public" if channel == 0 else "идут в #tulameshbridge", text)

    def test_status_uptime(self):
        self.radio.nodes = {"node": {}}
        with patch.object(gateway, "time", SimpleNamespace(monotonic=lambda: gateway.bridge.STARTED_AT + 61)):
            text = gateway.status_text(self.radio)

        self.assertIn("моста: 0 д 00:01:01", text)
        self.assertIn("Известных контактов: 1", text)


if __name__ == "__main__":
    unittest.main()
