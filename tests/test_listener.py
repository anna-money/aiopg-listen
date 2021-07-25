import asyncio
import contextlib
from typing import Any, Dict, List

import aiopg

import aiopg_listen


class TestHandler:
    def __init__(self, delay: float = 0) -> None:
        self.delay = delay
        self.notifications: List[aiopg_listen.NotificationOrTimeout] = []

    async def handle(self, notification: aiopg_listen.NotificationOrTimeout) -> None:
        await asyncio.sleep(self.delay)
        self.notifications.append(notification)


async def cancel_and_wait(future: asyncio.Future[None]) -> None:
    future.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await future


async def test_two_inactive_channels(pg_server: Dict[str, Any]) -> None:
    handler_1 = TestHandler()
    handler_2 = TestHandler()
    consumer = aiopg_listen.NotificationListener(aiopg_listen.connect_func(**pg_server["pg_params"]))
    consume_task = asyncio.create_task(
        consumer.run({"inactive_1": handler_1.handle, "inactive_2": handler_2.handle}, notification_timeout=1)
    )

    await asyncio.sleep(1.5)
    await cancel_and_wait(consume_task)

    assert handler_1.notifications == [aiopg_listen.Timeout("inactive_1")]
    assert handler_2.notifications == [aiopg_listen.Timeout("inactive_2")]


async def test_one_active_channel_and_one_passive_channel(pg_server: Dict[str, Any]) -> None:
    active_handler = TestHandler()
    inactive_handler = TestHandler()
    consumer = aiopg_listen.NotificationListener(aiopg_listen.connect_func(**pg_server["pg_params"]))
    consume_task = asyncio.create_task(
        consumer.run({"active": active_handler.handle, "inactive": inactive_handler.handle}, notification_timeout=1)
    )

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        await asyncio.sleep(0.75)
        await cursor.execute("NOTIFY active, '1'")
        await cursor.execute("NOTIFY active, '2'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert active_handler.notifications == [
        aiopg_listen.Notification("active", "1"),
        aiopg_listen.Notification("active", "2"),
    ]
    assert inactive_handler.notifications == [
        aiopg_listen.Timeout("inactive"),
    ]


async def test_two_active_channels(pg_server: Dict[str, Any]) -> None:
    handler_1 = TestHandler()
    handler_2 = TestHandler()
    consumer = aiopg_listen.NotificationListener(aiopg_listen.connect_func(**pg_server["pg_params"]))
    consume_task = asyncio.create_task(
        consumer.run({"active_1": handler_1.handle, "active_2": handler_2.handle}, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        await cursor.execute("NOTIFY active_1, '1'")
        await cursor.execute("NOTIFY active_2, '2'")
        await cursor.execute("NOTIFY active_2, '3'")
        await cursor.execute("NOTIFY active_1, '4'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert handler_1.notifications == [
        aiopg_listen.Notification("active_1", "1"),
        aiopg_listen.Notification("active_1", "4"),
    ]
    assert handler_2.notifications == [
        aiopg_listen.Notification("active_2", "2"),
        aiopg_listen.Notification("active_2", "3"),
    ]


async def test_listen_policy_last(pg_server: Dict[str, Any]) -> None:
    handler = TestHandler(delay=0.1)
    consumer = aiopg_listen.NotificationListener(aiopg_listen.connect_func(**pg_server["pg_params"]))
    consume_task = asyncio.create_task(
        consumer.run({"simple": handler.handle}, policy=aiopg_listen.ListenPolicy.LAST, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        for i in range(10):
            await cursor.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert handler.notifications == [
        aiopg_listen.Notification("simple", "0"),
        aiopg_listen.Notification("simple", "9"),
    ]


async def test_listen_policy_all(pg_server: Dict[str, Any]) -> None:
    handler = TestHandler(delay=0.05)
    consumer = aiopg_listen.NotificationListener(aiopg_listen.connect_func(**pg_server["pg_params"]))
    consume_task = asyncio.create_task(consumer.run({"simple": handler.handle}, notification_timeout=1))
    await asyncio.sleep(0.1)

    async with aiopg.connect(**pg_server["pg_params"]) as connection, connection.cursor() as cursor:
        for i in range(10):
            await cursor.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)

    await cancel_and_wait(consume_task)

    assert handler.notifications == [aiopg_listen.Notification("simple", str(i)) for i in range(10)]


async def test_failed_to_connect(pg_server: Dict[str, Any]) -> None:
    async def connect() -> aiopg.Connection:
        raise RuntimeError("Failed to connect")

    handler = TestHandler()
    consumer = aiopg_listen.NotificationListener(connect)
    consume_task = asyncio.create_task(consumer.run({"simple": handler.handle}, notification_timeout=1))
    await asyncio.sleep(1.5)
    await cancel_and_wait(consume_task)

    assert handler.notifications == [aiopg_listen.Timeout("simple")]
