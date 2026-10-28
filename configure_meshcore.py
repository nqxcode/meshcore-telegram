import asyncio
import time

from meshcore import EventType, MeshCore


async def main():
    radio = await MeshCore.create_serial("/dev/ttyACM0", default_timeout=10)
    if not radio or not radio.self_info:
        raise RuntimeError("MeshCore не отвечает по USB")

    try:
        commands = [
            ("radio", lambda: radio.commands.set_radio(868.731, 62.5, 8, 7)),
            ("name", lambda: radio.commands.set_name("TulaMeshBridge")),
            ("channel", lambda: radio.commands.set_channel(1, "#tulameshbridge")),
            ("path hash", lambda: radio.commands.set_path_hash_mode(1)),
            ("time", lambda: radio.commands.set_time(int(time.time()))),
        ]
        for label, command in commands:
            result = await command()
            if result is None or result.type != EventType.OK:
                raise RuntimeError(f"Не удалось настроить {label}: {result}")

            print(label, "OK", flush=True)

        await radio.commands.reboot()
    finally:
        await radio.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
