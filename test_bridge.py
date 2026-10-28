import importlib
import json
import os
import queue
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


    def test_duration_boundaries(self):
        for seconds, expected in ((-1, "0 д 00:00:00"), (0, "0 д 00:00:00"),
                                  (90061.9, "1 д 01:01:01")):
            with self.subTest(seconds=seconds):
                self.assertEqual(bridge.duration(seconds), expected)

    def test_first_message_does_not_depend_on_clock_origin(self):
        with patch.object(bridge.time, "time", return_value=0):
            bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_called_once()

    def test_rate_limit_reply_rounds_up_remaining_seconds(self):
        self.state["last_send"] = 1000
        with patch.object(bridge, "SEND_INTERVAL", 2), patch.object(bridge.time, "time", return_value=1001):
            bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_not_called()
        self.assertEqual(self.telegram.call_args.kwargs["text"], "Ограничение частоты: повторите через 1 с.")

    def test_status_reports_uptime_and_disconnection(self):
        self.interface.nodes["!1"]["deviceMetrics"] = {"uptimeSeconds": 61}
        self.interface.isConnected.is_set.return_value = False
        with patch.object(bridge.time, "monotonic", return_value=bridge.STARTED_AT + 3601):
            text = bridge.status_text(self.interface)

        self.assertIn("Heltec: отключён", text)
        self.assertIn("моста: 0 д 01:00:01", text)
        self.assertIn("телеметрия): 0 д 00:01:01", text)

    def test_send_accepts_exact_utf8_limit_and_addressed_command(self):
        text = "/send@MESH_BOT Привет"
        payload = "[TG Андрей] Привет"
        with patch.object(bridge, "MAX_MESSAGE_BYTES", len(payload.encode("utf-8"))):
            bridge.handle_message(self.interface, self.message(text), self.state)

        self.interface.sendText.assert_called_once_with(payload, channelIndex=bridge.CHANNEL)

    def test_duplicate_cache_expires_at_one_second(self):
        digest = bridge.hashlib.sha256("Привет".encode()).hexdigest()
        self.state.update(last_send=998, recent={digest: 999})
        with patch.object(bridge.time, "time", return_value=1000):
            bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_called_once()
        self.assertEqual(json.loads(self.state_path.read_text())["recent"], {digest: 1000})

    def test_radio_filters_and_default_channel(self):
        for packet in (
            {"from": 2},
            {"from": 2, "decoded": {"text": ""}},
            {"from": 2, "channel": bridge.CHANNEL + 1, "decoded": {"text": "Нет"}},
            {"from": 1, "channel": bridge.CHANNEL, "decoded": {"text": "Эхо"}},
        ):
            bridge.on_receive(packet, self.interface)

        self.assertTrue(bridge.messages.empty())
        with patch.object(bridge, "CHANNEL", 0):
            bridge.on_receive({"from": 2, "decoded": {"text": "Без канала"}}, self.interface)

        self.assertEqual(bridge.messages.get_nowait(), ("unknown", "Без канала"))

    def test_radio_deduplication_expires_and_evicts_oldest(self):
        packet = {"from": 2, "id": 8, "decoded": {"text": "Привет"}, "channel": bridge.CHANNEL}
        with patch.object(bridge.time, "monotonic", return_value=1000):
            bridge.on_receive(packet, self.interface)
        with patch.object(bridge.time, "monotonic", return_value=1300):
            bridge.on_receive(packet, self.interface)

        self.assertEqual(bridge.messages.qsize(), 2)
        bridge.seen_packets.clear()
        bridge.seen_packets.update(((2, i), 1300) for i in range(4096))
        packet["id"] = 4096
        with patch.object(bridge.time, "monotonic", return_value=1300):
            bridge.on_receive(packet, self.interface)

        self.assertEqual(len(bridge.seen_packets), 4096)
        self.assertNotIn((2, 0), bridge.seen_packets)
        self.assertIn((2, 1), bridge.seen_packets)
        self.assertIn((2, 4096), bridge.seen_packets)

    def test_full_radio_queue_drops_new_message(self):
        with patch.object(bridge, "messages", queue.Queue(maxsize=1)):
            bridge.messages.put(("first", "Сохранить"))

            bridge.on_receive({"from": 2, "decoded": {"text": "Новое"}}, self.interface)

            self.assertEqual(bridge.messages.qsize(), 1)
            self.assertEqual(bridge.messages.get_nowait(), ("first", "Сохранить"))

    def test_single_node_page_has_no_page_counter(self):
        pages = bridge.node_list_pages({"!1": {"user": {"longName": "Один"}}})

        self.assertEqual(len(pages), 1)
        self.assertNotIn("·", pages[0])
        self.assertIn("1. Один", pages[0])

    def test_node_sort_uses_name_then_identifier(self):
        nodes = {
            "0": {"user": {"longName": "Zulu"}},
            "9": {"user": {"longName": "alpha"}},
            "8": {"user": {"longName": "alpha"}},
        }

        text = bridge.node_list_pages(nodes)[0]

        self.assertLess(text.index("ID: 8"), text.index("ID: 9"))
        self.assertLess(text.index("ID: 9"), text.index("ID: 0"))

    def test_pages_accept_exact_size_and_reset_size_for_next_page(self):
        nodes = {str(index): {"user": {"longName": str(index) * 873}} for index in range(4)}

        pages = bridge.node_list_pages(nodes)

        self.assertEqual([page.count("ID:") for page in pages], [2, 2])
        self.assertIn("3. " + "2" * 873, pages[1])

    def test_pages_split_when_total_exceeds_size_limit(self):
        nodes = {str(index): {"user": {"longName": str(index) * 874}} for index in range(2)}

        pages = bridge.node_list_pages(nodes)

        self.assertEqual([page.count("ID:") for page in pages], [1, 1])

    def test_radio_cache_expiry_keeps_recent_packets(self):
        bridge.seen_packets.update([((2, 1), 999), ((2, 2), 1001)])
        packet = {"from": 2, "id": 3, "decoded": {"text": "Новое"}, "channel": bridge.CHANNEL}
        with patch.object(bridge.time, "monotonic", return_value=1300):
            bridge.on_receive(packet, self.interface)

        self.assertEqual(list(bridge.seen_packets), [(2, 2), (2, 3)])

    def test_radio_cache_accepts_exact_capacity(self):
        bridge.seen_packets.update(((2, index), 1000) for index in range(4095))
        packet = {"from": 2, "id": 4095, "decoded": {"text": "Новое"}, "channel": bridge.CHANNEL}
        with patch.object(bridge.time, "monotonic", return_value=1000):
            bridge.on_receive(packet, self.interface)

        self.assertEqual(len(bridge.seen_packets), 4096)
        self.assertIn((2, 0), bridge.seen_packets)

    def test_attempt_is_persisted_before_radio_send(self):
        def check_state(*args, **kwargs):
            saved = json.loads(self.state_path.read_text())
            self.assertEqual(saved["last_send"], 1000)
            self.assertIn(bridge.hashlib.sha256("Привет".encode()).hexdigest(), saved["recent"])

        self.interface.sendText.side_effect = check_state
        with patch.object(bridge.time, "time", return_value=1000):
            bridge.handle_message(self.interface, self.message(), self.state)

        self.interface.sendText.assert_called_once()


if __name__ == "__main__":
    unittest.main()
