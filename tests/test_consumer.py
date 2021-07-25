import asyncio
import contextlib
from typing import Any, Dict, List, Union

import aiopg

import aiopg_listen


class Processor:
    def __init__(self, delay: float = 0) -> None:
        self.delay = delay
        self.notifications: List[Union[aiopg_listen.Notification, aiopg_listen.Timeout]] = []

    async def process(self, notification: Union[aiopg_listen.Notification, aiopg_listen.Timeout]) -> None:
        await asyncio.sleep(self.delay)
        self.notifications.append(notification)


async def cancel_and_wait(future: asyncio.Future[None]) -> None:
    future.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await future


async def test_two_inactive_channels(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        return await aiopg.connect(**pg_server["pg_params"])

    processor_1 = Processor()
    processor_2 = Processor()
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(
        consumer.consume({"inactive_1": processor_1.process, "inactive_2": processor_2.process}, notification_timeout=1)
    )

    await asyncio.sleep(1.5)
    await cancel_and_wait(consume_task)

    assert processor_1.notifications == [aiopg_listen.Timeout("inactive_1")]
    assert processor_2.notifications == [aiopg_listen.Timeout("inactive_2")]


async def test_one_active_channel_and_one_passive_channel(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        return await aiopg.connect(**pg_server["pg_params"])

    active_processor = Processor()
    inactive_processor = Processor()
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(
        consumer.consume(
            {"active": active_processor.process, "inactive": inactive_processor.process}, notification_timeout=1
        )
    )

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        await asyncio.sleep(0.75)
        await cursor.execute("NOTIFY active, '1'")
        await cursor.execute("NOTIFY active, '2'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert active_processor.notifications == [
        aiopg_listen.Notification("active", "1"),
        aiopg_listen.Notification("active", "2"),
    ]
    assert inactive_processor.notifications == [
        aiopg_listen.Timeout("inactive"),
    ]


async def test_two_active_channels(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        return await aiopg.connect(**pg_server["pg_params"])

    processor_1 = Processor()
    processor_2 = Processor()
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(
        consumer.consume({"active_1": processor_1.process, "active_2": processor_2.process}, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        await cursor.execute("NOTIFY active_1, '1'")
        await cursor.execute("NOTIFY active_2, '2'")
        await cursor.execute("NOTIFY active_2, '3'")
        await cursor.execute("NOTIFY active_1, '4'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert processor_1.notifications == [
        aiopg_listen.Notification("active_1", "1"),
        aiopg_listen.Notification("active_1", "4"),
    ]
    assert processor_2.notifications == [
        aiopg_listen.Notification("active_2", "2"),
        aiopg_listen.Notification("active_2", "3"),
    ]


async def test_consume_policy_last(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        return await aiopg.connect(**pg_server["pg_params"])

    processor = Processor(delay=0.1)
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(
        consumer.consume({"simple": processor.process}, policy=aiopg_listen.ConsumePolicy.LAST, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        for i in range(10):
            await cursor.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert processor.notifications == [
        aiopg_listen.Notification("simple", "0"),
        aiopg_listen.Notification("simple", "9"),
    ]


async def test_consume_policy_all(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        return await aiopg.connect(**pg_server["pg_params"])

    processor = Processor(delay=0.05)
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(consumer.consume({"simple": processor.process}, notification_timeout=1))
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        for i in range(10):
            await cursor.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert processor.notifications == [aiopg_listen.Notification("simple", str(i)) for i in range(10)]


async def test_failed_to_connect(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        raise RuntimeError("Failed to connect")

    processor = Processor()
    consumer = aiopg_listen.NotificationConsumer(connect)
    consume_task = asyncio.create_task(consumer.consume({"simple": processor.process}, notification_timeout=1))
    await asyncio.sleep(1.5)
    await cancel_and_wait(consume_task)

    assert processor.notifications == [aiopg_listen.Timeout("simple")]
