import asyncio
import hashlib
import json
import os
import queue
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch

with patch.dict(os.environ, TELEGRAM_BOT_TOKEN="test", TELEGRAM_CHAT_ID="123"):
    import bridge
    import configure_meshcore
    import meshcore_bridge as gateway


class TelegramTests(unittest.TestCase):
    def test_request_returns_result_and_checks_http_status(self):
        response = Mock()
        response.json.return_value = {"ok": True, "result": [1, 2]}
        with patch.object(bridge.session, "post", return_value=response) as post:
            result = bridge.telegram("getUpdates", offset=17)

        self.assertEqual(result, [1, 2])
        post.assert_called_once_with(f"{bridge.API}/getUpdates", json={"offset": 17}, timeout=35)
        response.raise_for_status.assert_called_once_with()

    def test_api_and_http_errors_propagate(self):
        response = Mock()
        response.json.return_value = {"ok": False, "description": "Forbidden"}
        with patch.object(bridge.session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Forbidden"):
                bridge.telegram("sendMessage")

            response.raise_for_status.side_effect = OSError("HTTP unavailable")
            with self.assertRaisesRegex(OSError, "HTTP unavailable"):
                bridge.telegram("getUpdates")


class RadioLifecycleTests(unittest.TestCase):
    def companion(self):
        commands = SimpleNamespace(
            get_channel=AsyncMock(side_effect=lambda index: SimpleNamespace(
                type=gateway.EventType.CHANNEL_INFO,
                payload={
                    "channel_name": gateway.CHANNELS[index],
                    "channel_secret": (gateway.PUBLIC_SECRET if index == 0 else
                                       hashlib.sha256(gateway.CHANNELS[index].encode()).digest()[:16]).hex(),
                },
            )),
            set_time=AsyncMock(return_value=SimpleNamespace(type=gateway.EventType.OK)),
            get_contacts=AsyncMock(return_value=SimpleNamespace(
                type=gateway.EventType.CONTACTS,
                payload={"abcdef0123456789": {"adv_name": "Сосед"}, "unknown": {}},
            )),
            send_chan_msg=AsyncMock(return_value=SimpleNamespace(type=gateway.EventType.OK)),
        )
        return SimpleNamespace(
            is_connected=True,
            self_info={"name": "Шлюз"},
            commands=commands,
            subscribe=Mock(),
            start_auto_message_fetching=AsyncMock(),
            disconnect=AsyncMock(),
        )

    def test_connect_send_timeout_and_close(self):
        companion = self.companion()
        with patch.object(gateway.MeshCore, "create_serial", AsyncMock(return_value=companion)) as create:
            radio = gateway.MeshCoreRadio()

        try:
            self.assertTrue(radio.isConnected.is_set())
            self.assertTrue(radio.thread.daemon)
            self.assertEqual(radio.name, "Шлюз")
            self.assertEqual(radio.nodes["abcdef0123456789"]["user"], {
                "longName": "Сосед",
                "shortName": "abcdef012345",
            })
            self.assertEqual(radio.nodes["unknown"]["user"]["longName"], "Без имени")
            create.assert_awaited_once_with(bridge.PORT, default_timeout=10)
            self.assertEqual(companion.commands.get_channel.await_args_list, [call(0), call(gateway.CHANNEL)])
            companion.start_auto_message_fetching.assert_awaited_once_with()
            self.assertEqual(companion.subscribe.call_args_list, [
                call(gateway.EventType.CHANNEL_MSG_RECV, radio.on_message),
                call(gateway.EventType.DISCONNECTED, radio.on_disconnect),
            ])

            radio.sendText("Привет", radio.channel)
            companion.commands.send_chan_msg.assert_awaited_once_with(radio.channel, "Привет")
            with self.assertRaisesRegex(RuntimeError, "неверный канал"):
                radio.sendText("Запрет", radio.channel + 1)

            with self.assertRaisesRegex(RuntimeError, "Тайм-аут"):
                radio.call(asyncio.sleep(60), timeout=0.001)
            radio.call(asyncio.sleep(0))

            radio.call(radio.on_disconnect(None))
            self.assertFalse(radio.isConnected.is_set())
            with self.assertRaises(RuntimeError):
                radio.sendText("Запрет", radio.channel)
            self.assertEqual(companion.commands.send_chan_msg.await_count, 1)
        finally:
            radio.close()
            radio.loop.close()

        self.assertFalse(radio.thread.is_alive())
        companion.disconnect.assert_awaited_once_with()

    def test_invalid_index_never_opens_usb(self):
        with patch.object(gateway.MeshCore, "create_serial", AsyncMock()) as create:
            for channel in (-1, 999):
                with self.subTest(channel=channel), self.assertRaises(ValueError):
                    gateway.MeshCoreRadio(channel)
            with patch.object(gateway, "CHANNEL", 0), self.assertRaises(ValueError):
                gateway.MeshCoreRadio(0)

        create.assert_not_awaited()

    def test_invalid_companion_stops_background_thread(self):
        for companion in (None, SimpleNamespace(is_connected=False, disconnect=AsyncMock()),
                          SimpleNamespace(is_connected=True, self_info={}, disconnect=AsyncMock())):
            with self.subTest(companion=companion):
                loop = asyncio.new_event_loop()
                with patch.object(gateway.asyncio, "new_event_loop", return_value=loop):
                    with patch.object(gateway.MeshCore, "create_serial", AsyncMock(return_value=companion)):
                        with self.assertRaisesRegex(RuntimeError, "Нет ответа"):
                            gateway.MeshCoreRadio()

                self.assertFalse(loop.is_running())
                loop.close()


class StateAndMainTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "state.json"
        for name, value in {
            "STATE_PATH": self.path,
            "messages": queue.Queue(),
            "bot_username": "",
            "CHANNEL": gateway.CHANNEL,
            "MAX_MESSAGE_BYTES": 180,
        }.items():
            patcher = patch.object(bridge, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_first_start_skips_backlog_and_preserves_existing_state(self):
        for updates, expected in (([], 0), ([{"update_id": 400}, {"update_id": 500}], 501)):
            with self.subTest(updates=updates):
                self.path.unlink(missing_ok=True)
                with patch.object(bridge, "telegram", return_value=updates) as telegram:
                    self.assertEqual(gateway.load_state(), {"offset": expected})
                    telegram.assert_called_once_with("getUpdates", offset=-1, timeout=0, allowed_updates=["message"])

                    self.assertEqual(gateway.load_state(), {"offset": expected})
                    self.assertEqual(telegram.call_count, 1)

        self.path.write_text(json.dumps({"offset": 1, "meshcore_channel": 999}))
        with self.assertRaisesRegex(ValueError, "неизвестный канал"):
            gateway.load_state()

    def test_meshcore_main_routes_selected_channel_refreshes_and_closes(self):
        radio = Mock(channel=0, nodes={})
        radio.isConnected.is_set.side_effect = [True, False]
        radio.refresh_contacts = AsyncMock()
        radio.call = asyncio.run
        bridge.messages.put((gateway.CHANNEL, "Пропустить"))
        bridge.messages.put((0, "Привет"))
        with patch.object(gateway, "MeshCoreRadio", return_value=radio) as create:
            with patch.object(gateway, "load_state", return_value={"offset": 5, "meshcore_channel": 0}):
                with patch.object(bridge, "telegram", return_value={"username": "mesh_bot"}):
                    with patch.object(bridge, "reply") as reply, patch.object(bridge, "receive_telegram") as receive:
                        with patch.object(gateway, "time", Mock(monotonic=Mock(side_effect=[1000, 1301, 1302]))):
                            with self.assertRaisesRegex(RuntimeError, "потеряно"):
                                gateway.main()

        create.assert_called_once_with(0)
        self.assertEqual(bridge.MAX_MESSAGE_BYTES, 120)
        self.assertEqual(bridge.bot_username, "mesh_bot")
        reply.assert_called_once_with("[MeshCore Public] Привет")
        receive.assert_called_once_with(radio, {"offset": 5, "meshcore_channel": 0},
                                        status_provider=gateway.status_text, message_handler=gateway.handle_message)
        radio.refresh_contacts.assert_awaited_once_with()
        radio.close.assert_called_once_with()

    def test_meshcore_main_recovers_from_telegram_errors(self):
        radio = Mock(channel=gateway.CHANNEL)
        radio.isConnected.is_set.side_effect = [True, False]
        bridge.messages.put((gateway.CHANNEL, "Привет"))
        with patch.object(gateway, "MeshCoreRadio", return_value=radio):
            with patch.object(gateway, "load_state", return_value={"offset": 0}):
                with patch.object(bridge, "telegram", return_value={"username": "mesh_bot"}):
                    with patch.object(bridge, "reply", side_effect=OSError("Telegram unavailable")):
                        with patch.object(bridge, "receive_telegram", side_effect=OSError("Polling unavailable")):
                            with patch.object(gateway.time, "monotonic", return_value=1000):
                                with patch.object(gateway.time, "sleep") as sleep:
                                    with self.assertRaisesRegex(RuntimeError, "потеряно"):
                                        gateway.main()

        sleep.assert_called_once_with(5)
        radio.close.assert_called_once_with()

    def test_meshtastic_main_initializes_state_and_recovers_from_poll_errors(self):
        serial = Mock()
        interface = serial.SerialInterface.return_value
        pub = Mock()
        modules = {
            "meshtastic": SimpleNamespace(serial_interface=serial),
            "meshtastic.serial_interface": serial,
            "pubsub": SimpleNamespace(pub=pub),
        }
        telegram = Mock(side_effect=lambda method, **data: (
            {"username": "mesh_bot"} if method == "getMe" else
            [{"update_id": 400}, {"update_id": 500}] if method == "getUpdates" else None
        ))
        bridge.messages.put(("!2", "Привет"))
        with patch.dict(sys.modules, modules), patch.object(bridge, "telegram", telegram):
            with patch.object(bridge, "receive_telegram", side_effect=[OSError("Polling"), KeyboardInterrupt]):
                with patch.object(bridge.time, "sleep") as sleep:
                    with self.assertRaises(KeyboardInterrupt):
                        bridge.main()

        self.assertEqual(json.loads(self.path.read_text()), {"offset": 501})
        serial.SerialInterface.assert_called_once_with(devPath=bridge.PORT)
        pub.subscribe.assert_called_once_with(bridge.on_receive, "meshtastic.receive")
        self.assertIn(call("sendMessage", chat_id=bridge.CHAT_ID, text="[LoRa !2] Привет"), telegram.call_args_list)
        sleep.assert_called_once_with(5)
        interface.close.assert_called_once_with()
        self.assertIn(call("getUpdates", offset=-1, timeout=0, allowed_updates=["message"]), telegram.call_args_list)

        self.path.unlink()
        telegram.reset_mock()
        telegram.side_effect = lambda method, **data: {"username": "mesh_bot"} if method == "getMe" else []
        with patch.dict(sys.modules, modules), patch.object(bridge, "telegram", telegram):
            with patch.object(bridge, "receive_telegram", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    bridge.main()

        self.assertEqual(json.loads(self.path.read_text()), {"offset": 0})

    def test_meshtastic_restart_keeps_state_and_survives_send_failure(self):
        state = {"offset": 123, "last_send": 1000, "recent": {"digest": 1000}}
        self.path.write_text(json.dumps(state))
        serial = Mock()
        modules = {
            "meshtastic": SimpleNamespace(serial_interface=serial),
            "meshtastic.serial_interface": serial,
            "pubsub": SimpleNamespace(pub=Mock()),
        }
        telegram = Mock(side_effect=[{"username": "mesh_bot"}, OSError("send failed")])
        bridge.messages.put(("!2", "Привет"))
        with patch.dict(sys.modules, modules), patch.object(bridge, "telegram", telegram):
            with patch.object(bridge, "receive_telegram", side_effect=KeyboardInterrupt) as receive:
                with self.assertRaises(KeyboardInterrupt):
                    bridge.main()

        receive.assert_called_once_with(serial.SerialInterface.return_value, state)
        self.assertEqual(json.loads(self.path.read_text()), state)
        self.assertEqual([c.args[0] for c in telegram.call_args_list], ["getMe", "sendMessage"])
        serial.SerialInterface.return_value.close.assert_called_once_with()

    def test_contacts_are_not_refreshed_at_exact_five_minutes(self):
        radio = Mock(channel=gateway.CHANNEL)
        radio.isConnected.is_set.side_effect = [True, False]
        with patch.object(gateway, "MeshCoreRadio", return_value=radio):
            with patch.object(gateway, "load_state", return_value={"offset": 0}):
                with patch.object(bridge, "telegram", return_value={"username": "mesh_bot"}):
                    with patch.object(bridge, "receive_telegram"):
                        with patch.object(gateway, "time", Mock(monotonic=Mock(side_effect=[1000, 1300]))):
                            with self.assertRaisesRegex(RuntimeError, "потеряно"):
                                gateway.main()

        radio.refresh_contacts.assert_not_called()
        radio.close.assert_called_once_with()


class ConfigureTests(unittest.IsolatedAsyncioTestCase):
    async def test_configure_parameters_and_disconnect(self):
        commands = SimpleNamespace(**{name: AsyncMock(return_value=SimpleNamespace(type=gateway.EventType.OK))
                                     for name in ("set_radio", "set_name", "set_channel", "set_path_hash_mode",
                                                  "set_time", "reboot")})
        radio = SimpleNamespace(self_info={"name": "Шлюз"}, commands=commands, disconnect=AsyncMock())
        with patch.object(configure_meshcore.MeshCore, "create_serial", AsyncMock(return_value=radio)):
            with patch.object(configure_meshcore.time, "time", return_value=1000.9), patch("builtins.print") as output:
                await configure_meshcore.main()

        commands.set_radio.assert_awaited_once_with(868.731, 62.5, 8, 7)
        commands.set_name.assert_awaited_once_with("TulaMeshBridge")
        commands.set_channel.assert_awaited_once_with(1, "#tulameshbridge")
        commands.set_path_hash_mode.assert_awaited_once_with(1)
        commands.set_time.assert_awaited_once_with(1000)
        commands.reboot.assert_awaited_once_with()
        radio.disconnect.assert_awaited_once_with()
        self.assertEqual(output.call_args_list, [
            call(label, "OK", flush=True) for label in ("radio", "name", "channel", "path hash", "time")
        ])

    async def test_configuration_rejection_does_not_reboot(self):
        for result in (None, SimpleNamespace(type=gateway.EventType.ERROR)):
            with self.subTest(result=result):
                radio = SimpleNamespace(
                    self_info={"name": "Шлюз"},
                    commands=SimpleNamespace(set_radio=AsyncMock(return_value=result), reboot=AsyncMock()),
                    disconnect=AsyncMock(),
                )
                with patch.object(configure_meshcore.MeshCore, "create_serial", AsyncMock(return_value=radio)):
                    with self.assertRaisesRegex(RuntimeError, "radio"):
                        await configure_meshcore.main()

                radio.commands.reboot.assert_not_awaited()
                radio.disconnect.assert_awaited_once_with()

    async def test_missing_companion_is_an_error(self):
        for radio in (None, SimpleNamespace(self_info={})):
            with self.subTest(radio=radio):
                with patch.object(configure_meshcore.MeshCore, "create_serial", AsyncMock(return_value=radio)):
                    with self.assertRaisesRegex(RuntimeError, "не отвечает"):
                        await configure_meshcore.main()
