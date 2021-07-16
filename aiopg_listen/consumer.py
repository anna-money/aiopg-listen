import asyncio
import datetime
import enum
import logging
from typing import Awaitable, Callable, Collection, Dict, Optional, Union

import aiopg
import async_timeout

logger = logging.getLogger(__package__)


class Policy(str, enum.Enum):
    ALL = "ALL"
    LAST = "LAST"

    def __str__(self) -> str:
        return self.value


class Timeout:
    __slots__ = ("_channel",)

    def __init__(self, channel: str):
        self._channel = channel


class Notify:
    __slots__ = ("channel", "payload")

    def __init__(self, channel: str, payload: Optional[str]):
        self.channel = channel
        self.payload = payload


ConsumeFunc = Callable[[Union[Notify, Timeout]], Awaitable]


class NotifiesConsumer:
    __slots__ = ("_connect", "_reconnect_delay")

    def __init__(self, connect: Callable[[], aiopg.Connection], reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect

    async def run(
        self,
        channels: Dict[str, ConsumeFunc],
        *,
        policy: Policy = Policy.ALL,
        notify_timeout: float = 30,
    ) -> None:
        notifies = asyncio.Queue[Notify]()

        listen_notifies_task = asyncio.create_task(
            self._listen_notifies(notifies, channels=channels.keys()), name=__package__
        )
        process_notifies_task = asyncio.create_task(
            self._process_notifies(notifies, channels=channels, policy=policy, notify_timeout=notify_timeout),
            name=__package__,
        )
        await asyncio.gather(listen_notifies_task, process_notifies_task)

    @staticmethod
    async def _process_notifies(
        notifies: asyncio.Queue[Notify], *, channels: Dict[str, ConsumeFunc], policy: Policy, notify_timeout: float
    ) -> None:
        while True:
            last_seen_at_by_channel: Dict[str, datetime.datetime] = {}
            last_seen_notify_by_channel: Dict[str, Union[Notify, Timeout]] = {}
            while policy == Policy.LAST and not notifies.empty():
                notify = notifies.get_nowait()
                last_seen_notify_by_channel[notify.channel] = notify
                last_seen_at_by_channel[notify.channel] = datetime.datetime.utcnow()
            else:
                try:
                    with async_timeout.timeout(timeout=notify_timeout):
                        notify = await notifies.get()
                        last_seen_notify_by_channel[notify.channel] = notify
                        last_seen_at_by_channel[notify.channel] = datetime.datetime.utcnow()
                except asyncio.TimeoutError:
                    for channel, last_seen_at in last_seen_at_by_channel.items():
                        if datetime.datetime.utcnow() - last_seen_at < datetime.timedelta(seconds=notify_timeout):
                            continue

                        last_seen_notify_by_channel[notify.channel] = Timeout(channel)
                        last_seen_at_by_channel[notify.channel] = datetime.datetime.utcnow()

            for channel, last_seen_notify in last_seen_notify_by_channel.items():
                handler = channels.get(channel)
                if handler is None:
                    continue
                await asyncio.create_task(handler(last_seen_notify), name=__package__)

    async def _listen_notifies(self, notifies: asyncio.Queue[Notify], *, channels: Collection[str]) -> None:
        failed_connect_attempts = 0
        while True:
            try:
                connection = await self._connect()
                try:
                    async with connection.cursor() as cursor:
                        for channel in channels:
                            await cursor.execute(f"LISTEN {channel}")

                    failed_connect_attempts = 0
                    while True:
                        notify = await connection.notifies.get()
                        await notifies.put(Notify(notify.channel, notify.payload))
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception("Connection was lost or not established")

                await asyncio.sleep(self._reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1
