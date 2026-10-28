import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


with patch.dict(os.environ, TELEGRAM_BOT_TOKEN="test", TELEGRAM_CHAT_ID="123"):
    bridge = importlib.import_module("bridge")


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state_path = Path(self.directory.name) / "state.json"
        for name, value in {
            "STATE_PATH": self.state_path,
            "CHAT_ID": 123,
            "SEND_INTERVAL": 1,
            "bot_username": "mesh_bot",
        }.items():
            patcher = patch.object(bridge, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.interface = Mock()
        self.interface.isConnected.is_set.return_value = True
        self.interface.myInfo = SimpleNamespace(my_node_num=1)
        self.interface.nodes = {"!1": {"num": 1, "user": {"longName": "Heltec"}}}
        self.state = {"offset": 0}
        patcher = patch.object(bridge, "telegram")
        self.telegram = patcher.start()
        self.addCleanup(patcher.stop)
        bridge.seen_packets.clear()
        while not bridge.messages.empty():
            bridge.messages.get_nowait()

    def message(self, text="/send Привет", user_id=42, **extra):
        return {
            "chat": {"id": 123},
            "from": {"id": user_id, "first_name": "Андрей"},
            "text": text,
            **extra,
        }

    def test_send_rejects_invalid_messages(self):
        cases = [
            self.message(chat={"id": 456}),
            self.message("Обычный текст", chat={"id": 456}),
            self.message("Обычный текст", **{"from": {"is_bot": True}}),
            self.message("   "),
            self.message("я" * 100),
            self.message("/send@another_bot Привет"),
            self.message("/send"),
            self.message("/send " + "я" * 100),
        ]
        for message in cases:
            with self.subTest(message=message):
                bridge.handle_message(self.interface, message, self.state)

        self.interface.sendText.assert_not_called()

    def test_plain_text_is_forwarded(self):
        for text in ("Hello Mesh 8", "user@example.org привет", "@someone привет", "/unknown привет"):
            with self.subTest(text=text):
                self.interface.sendText.reset_mock()

                bridge.handle_message(self.interface, self.message(text), {"offset": 0})

                self.interface.sendText.assert_called_once_with(
                    f"[TG Андрей] {text}", channelIndex=bridge.CHANNEL
                )

    def test_send_allows_any_group_member(self):
        bridge.handle_message(self.interface, self.message(user_id=7), self.state)

        self.interface.sendText.assert_called_once_with(
            "[TG Андрей] Привет", channelIndex=bridge.CHANNEL
        )

    def test_rate_limit_and_duplicate_survive_reload(self):
        with patch.object(bridge.time, "time", return_value=1000):
            bridge.handle_message(self.interface, self.message("Привет"), self.state)

        state = json.loads(self.state_path.read_text())
        with patch.object(bridge.time, "time", return_value=1000.999):
            bridge.handle_message(self.interface, self.message("Привет"), state)
            bridge.handle_message(self.interface, self.message("Другое"), state)

        self.interface.sendText.assert_called_once()
        self.assertIn("последнюю секунду", self.telegram.call_args_list[-2].kwargs["text"])
        self.assertIn("Ограничение частоты", self.telegram.call_args_list[-1].kwargs["text"])

        with patch.object(bridge.time, "time", return_value=1001):
            bridge.handle_message(self.interface, self.message("Привет"), state)

        self.assertEqual(self.interface.sendText.call_count, 2)
        with patch.object(bridge.time, "time", return_value=1002):
            bridge.handle_message(self.interface, self.message("Другое"), state)

        self.assertEqual(self.interface.sendText.call_count, 3)
        self.interface.sendText.assert_called_with("[TG Андрей] Другое", channelIndex=bridge.CHANNEL)

    def test_failed_reply_and_repeated_update_do_not_repeat_radio_send(self):
        update = {"update_id": 10, "message": self.message()}
        self.telegram.side_effect = [[update], RuntimeError("reply failed"), [update]]
        bridge.receive_telegram(self.interface, self.state)
        restored = json.loads(self.state_path.read_text())
        bridge.receive_telegram(self.interface, restored)

        self.interface.sendText.assert_called_once()
        self.assertEqual(restored["offset"], 11)

    def test_state_write_failure_prevents_radio_send(self):
        with patch.object(bridge, "save_state", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_not_called()

    def test_radio_failure_is_not_retried_after_reload(self):
        self.interface.sendText.side_effect = OSError("serial disconnected")
        bridge.handle_message(self.interface, self.message(), self.state)
        restored = json.loads(self.state_path.read_text())
        bridge.handle_message(self.interface, self.message(), restored)

        self.interface.sendText.assert_called_once()

    def test_disconnected_radio_does_not_send(self):
        self.interface.isConnected.is_set.return_value = False
        bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_not_called()

    def test_status_and_nodes_do_not_transmit(self):
        for command in ("/status", "/nodes"):
            bridge.handle_message(self.interface, self.message(command, user_id=7), self.state)

        self.interface.sendText.assert_not_called()
        responses = [call.kwargs["text"] for call in self.telegram.call_args_list]
        self.assertIn("нет данных", responses[0])
        self.assertIn("Известных узлов: 1", responses[0])
        self.assertIn("Heltec", responses[1])

    def test_status_and_nodes_reply_only_to_group(self):
        for command in ("/status", "/nodes", "/status@mesh_bot", "/nodes@mesh_bot"):
            with self.subTest(command=command):
                self.telegram.reset_mock()

                bridge.handle_message(self.interface, self.message(command, user_id=7), self.state)

                calls = self.telegram.call_args_list
                self.assertEqual([call.kwargs["chat_id"] for call in calls], [123])

        self.interface.sendText.assert_not_called()

    def test_command_reply_failure_has_no_fallback_to_another_chat(self):
        for command in ("/status", "/nodes"):
            for chat in ({"id": 123, "type": "group"}, {"id": 7, "type": "private"}):
                with self.subTest(command=command, chat=chat):
                    self.telegram.reset_mock()
                    self.telegram.side_effect = RuntimeError("Forbidden")

                    with self.assertRaises(RuntimeError):
                        bridge.handle_message(self.interface, self.message(command, user_id=7, chat=chat), self.state)

                    self.telegram.assert_called_once()
                    self.assertEqual(self.telegram.call_args.kwargs["chat_id"], chat["id"])

    def test_confirmation_goes_to_sender(self):
        bridge.handle_message(self.interface, self.message(user_id=7), self.state)

        self.telegram.assert_called_once_with("sendMessage", chat_id=7, text="Передано в mesh-сеть:\n[TG Андрей] Привет")

    def test_command_and_error_responses_go_to_sender(self):
        for text in ("/send", "я" * 200):
            with self.subTest(text=text):
                self.telegram.reset_mock()

                bridge.handle_message(self.interface, self.message(text, user_id=7), self.state)

                self.assertTrue(self.telegram.called)
                self.assertTrue(all(call.kwargs["chat_id"] == 7 for call in self.telegram.call_args_list))

    def test_unavailable_private_chat_has_no_group_fallback(self):
        self.telegram.side_effect = RuntimeError("Forbidden")

        with self.assertRaises(RuntimeError):
            bridge.handle_message(self.interface, self.message(user_id=7), self.state)

        self.interface.sendText.assert_called_once()
        self.telegram.assert_called_once_with("sendMessage", chat_id=7, text="Передано в mesh-сеть:\n[TG Андрей] Привет")

    def test_anonymous_sender_gets_no_private_reply(self):
        bridge.reply_to(self.message(sender_chat={"id": 123}), "Ответ")
        bridge.reply_to(self.message(**{"from": {}}), "Ответ")

        self.telegram.assert_not_called()

    def test_radio_message_still_goes_to_group(self):
        bridge.reply("[MeshCore Public] Привет")

        self.telegram.assert_called_once_with("sendMessage", chat_id=123, text="[MeshCore Public] Привет")

    def test_node_list_is_readable_and_sorted_by_name(self):
        pages = bridge.node_list_pages({
            "b" * 64: {"user": {"longName": "Борис\nТула"}},
            "a" * 64: {"user": {"longName": "Анна", "shortName": "ANNA"}},
        })

        self.assertEqual(len(pages), 1)
        self.assertIn("1. Анна\nКороткое имя: ANNA\nID: " + "a" * 64, pages[0])
        self.assertIn("2. Борис Тула", pages[0])
        self.assertIn("a" * 64, pages[0])
        self.assertIn("Доступность узлов сейчас не проверена", pages[0])

    def test_node_list_pages_do_not_split_entries(self):
        nodes = {str(index): {"user": {"longName": "Узел" + "я" * 100}} for index in range(23)}

        pages = bridge.node_list_pages(nodes)

        self.assertEqual(len(pages), 3)
        self.assertEqual([page.count("ID:") for page in pages], [10, 10, 3])
        self.assertTrue(all(len(page.encode("utf-16-le")) // 2 < 2000 for page in pages))
        self.assertIn("· 3/3", pages[-1])
        self.assertIn("23. ", pages[-1])

    def test_node_list_preserves_long_fields_when_paginating(self):
        name = "Узел " + "я" * 100
        nodes = {f"{index:064x}": {"user": {"longName": name, "shortName": "NODE"}} for index in range(20)}

        pages = bridge.node_list_pages(nodes)
        combined = "\n".join(pages)

        self.assertGreater(len(pages), 2)
        self.assertTrue(all(len(page.encode("utf-16-le")) // 2 < 2000 for page in pages))
        self.assertEqual(combined.count(name), 20)
        self.assertEqual(combined.count("Короткое имя: NODE"), 20)
        for identifier in nodes:
            self.assertIn(identifier, combined)

    def test_node_list_empty_and_missing_names(self):
        self.assertIn("Список пока пуст", bridge.node_list_pages({})[0])
        self.assertIn("Без имени", bridge.node_list_pages({"!1": {}})[0])

    def test_radio_duplicates_are_filtered(self):
        packet = {"from": 2, "id": 5, "channel": bridge.CHANNEL, "decoded": {"text": "Привет"}}
        bridge.on_receive(packet, self.interface)
        bridge.on_receive(packet, self.interface)

        self.assertEqual(bridge.messages.qsize(), 1)


if __name__ == "__main__":
    unittest.main()
