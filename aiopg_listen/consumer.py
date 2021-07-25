import asyncio
import dataclasses
import enum
import logging
from typing import Awaitable, Callable, Dict, Optional, Union

import aiopg
import async_timeout

logger = logging.getLogger(__package__)


class ConsumePolicy(str, enum.Enum):
    ALL = "ALL"
    LAST = "LAST"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True)
class Timeout:
    __slots__ = ("channel",)

    channel: str


@dataclasses.dataclass(frozen=True)
class Notification:
    __slots__ = ("channel", "payload")

    channel: str
    payload: Optional[str]


ConsumeNotificationFunc = Callable[[Union[Notification, Timeout]], Awaitable]


class NotificationConsumer:
    __slots__ = ("_connect", "_reconnect_delay")

    def __init__(self, connect: Callable[[], Awaitable[aiopg.Connection]], reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect

    async def consume(
        self,
        channels: Dict[str, ConsumeNotificationFunc],
        *,
        policy: ConsumePolicy = ConsumePolicy.ALL,
        notification_timeout: float = 30,
    ) -> None:
        queue_per_channel = {channel: asyncio.Queue[Notification]() for channel in channels.keys()}

        read_notifications_task = asyncio.create_task(
            self._read_notifications(queue_per_channel=queue_per_channel), name=__package__
        )
        process_notifications_tasks = [
            asyncio.create_task(
                self._process_notifications(
                    channel,
                    notifications=queue_per_channel[channel],
                    handler=handler,
                    policy=policy,
                    notification_timeout=notification_timeout,
                ),
                name=f"{__package__}.{channel}",
            )
            for channel, handler in channels.items()
        ]
        try:
            await asyncio.gather(read_notifications_task, *process_notifications_tasks)
        finally:
            read_notifications_task.cancel()
            for process_notifications_task in process_notifications_tasks:
                process_notifications_task.cancel()

    @staticmethod
    async def _process_notifications(
        channel: str,
        *,
        notifications: asyncio.Queue[Notification],
        handler: ConsumeNotificationFunc,
        policy: ConsumePolicy,
        notification_timeout: float,
    ) -> None:
        while True:
            notification: Union[Notification, Timeout]

            if notifications.empty():
                try:
                    with async_timeout.timeout(timeout=notification_timeout):
                        notification = await notifications.get()
                except asyncio.TimeoutError:
                    notification = Timeout(channel)
            else:
                while not notifications.empty():
                    notification = notifications.get_nowait()
                    if policy == ConsumePolicy.ALL:
                        break

            # to have independent async context per run
            # to protect from misuse of contextvars
            await asyncio.create_task(handler(notification), name=__package__)

    async def _read_notifications(self, queue_per_channel: Dict[str, asyncio.Queue[Notification]]) -> None:
        failed_connect_attempts = 0
        while True:
            try:
                connection = await self._connect()
                try:
                    async with connection.cursor() as cursor:
                        for channel in queue_per_channel.keys():
                            await cursor.execute(f"LISTEN {channel}")

                    failed_connect_attempts = 0
                    while True:
                        notify = await connection.notifies.get()
                        queue = queue_per_channel.get(notify.channel)
                        if queue is None:
                            logger.warning("Queue is not found for channel %s", notify.channel)
                            continue

                        await queue.put(Notification(notify.channel, notify.payload))
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception("Connection was lost or not established")

                await asyncio.sleep(self._reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1
