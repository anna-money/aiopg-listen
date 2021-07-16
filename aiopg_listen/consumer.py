import asyncio
import datetime
import enum
import logging
from typing import Awaitable, Callable, Collection, Dict, Optional, Union

import aiopg
import async_timeout

logger = logging.getLogger(__package__)


class ConsumePolicy(str, enum.Enum):
    ALL = "ALL"
    LAST = "LAST"

    def __str__(self) -> str:
        return self.value


class Timeout:
    __slots__ = ("_channel",)

    def __init__(self, channel: str):
        self._channel = channel


class Notification:
    __slots__ = ("channel", "payload")

    def __init__(self, channel: str, payload: Optional[str]):
        self.channel = channel
        self.payload = payload


ConsumeNotificationFunc = Callable[[Union[Notification, Timeout]], Awaitable]


class NotificationConsumer:
    __slots__ = ("_connect", "_reconnect_delay")

    def __init__(self, connect: Callable[[], aiopg.Connection], reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect

    async def consume(
        self,
        channels: Dict[str, ConsumeNotificationFunc],
        *,
        policy: ConsumePolicy = ConsumePolicy.ALL,
        notification_timeout: float = 30,
    ) -> None:
        notifications = asyncio.Queue[Notification]()

        read_notifications_task = asyncio.create_task(
            self._read_notifications(notifications, channels=channels.keys()), name=__package__
        )
        process_notifications_task = asyncio.create_task(
            self._process_notifications(
                notifications, channels=channels, policy=policy, notification_timeout=notification_timeout
            ),
            name=__package__,
        )
        await asyncio.gather(read_notifications_task, process_notifications_task)

    @staticmethod
    async def _process_notifications(
        notifications: asyncio.Queue[Notification],
        *,
        channels: Dict[str, ConsumeNotificationFunc],
        policy: ConsumePolicy,
        notification_timeout: float,
    ) -> None:
        last_seen_at_by_channel: Dict[str, datetime.datetime] = {
            channel: datetime.datetime.utcnow() for channel in channels.keys()
        }
        while True:
            last_seen_notify_by_channel: Dict[str, Union[Notification, Timeout]] = {}
            while policy == ConsumePolicy.LAST and not notifications.empty():
                notification = notifications.get_nowait()
                last_seen_notify_by_channel[notification.channel] = notification
                last_seen_at_by_channel[notification.channel] = datetime.datetime.utcnow()
            else:
                try:
                    # TODO calculate timeout based on last_seen_at info to wait less
                    with async_timeout.timeout(timeout=notification_timeout):
                        notification = await notifications.get()
                        last_seen_notify_by_channel[notification.channel] = notification
                        last_seen_at_by_channel[notification.channel] = datetime.datetime.utcnow()
                except asyncio.TimeoutError:
                    pass  # notifications haven't been seen too long, last_seen_at should expire

            # should check last_seen_at for every channel to cover multiple cases:
            # 1. two low active channels: all handlers should receive timeout
            # 2. high active channel, low active: the handler of the second should receive timeout
            # 3. two high active channels: no timeout at all
            for channel, last_seen_at in last_seen_at_by_channel.items():
                if (datetime.datetime.utcnow() - last_seen_at).total_seconds() < notification_timeout:
                    continue

                last_seen_notify_by_channel[notification.channel] = Timeout(channel)
                last_seen_at_by_channel[notification.channel] = datetime.datetime.utcnow()

            for channel, last_seen_notify in last_seen_notify_by_channel.items():
                handler = channels.get(channel)
                if handler is None:
                    continue
                await asyncio.create_task(handler(last_seen_notify), name=__package__)

    async def _read_notifications(
        self, notifications: asyncio.Queue[Notification], *, channels: Collection[str]
    ) -> None:
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
                        await notifications.put(Notification(notify.channel, notify.payload))
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception("Connection was lost or not established")

                await asyncio.sleep(self._reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1
