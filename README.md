# aiopg-listen

This library simplifies usage of listen/notify with [aiopg](https://github.com/aio-libs/aiopg):
1. Handles lost of a connection
1. Simplifies processing notifications from multiple channels
1. Setups a timeout for receiving a notification
1. Allows to receive all notifications/only last notification depends on `ConsumePolicy`.

```python
import asyncio
import aiopg
import aiopg_listen

from typing import Union


async def process_notifications(notification: Union[aiopg_listen.Notification, aiopg_listen.Timeout]) -> None:
    print(f"{notification} has been received")


consumer = aiopg_listen.NotificationConsumer(aiopg.connect)
consume_task = asyncio.create_task(
    consumer.consume(
        {"channel": process_notifications},
        policy=aiopg_listen.ConsumePolicy.LAST,
        notification_timeout=1
    )
)

async with aiopg.connect() as connection, connection.cursor() as cursor:
    for i in range(42):
        await cursor.execute(f"NOTIFY simple, '{i}'")
```
