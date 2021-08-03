# aiopg-listen

This library simplifies usage of listen/notify with [aiopg](https://github.com/aio-libs/aiopg):
1. Handles lost of a connection
1. Simplifies processing notifications from multiple channels
1. Setups a timeout for receiving a notification
1. Allows to receive all notifications/only last notification depends on `ListenPolicy`.

```python
import asyncio
import aiopg
import aiopg_listen


async def handle_notifications(notification: aiopg_listen.NotificationOrTimeout) -> None:
    print(f"{notification} has been received")


listener = aiopg_listen.NotificationListener(aiopg_listen.connect_func())
listener_task = asyncio.create_task(
    listener.run(
        {"channel": handle_notifications},
        policy=aiopg_listen.ListenPolicy.LAST,
        notification_timeout=1
    )
)

async with aiopg.connect() as connection, connection.cursor() as cursor:
    for i in range(42):
        await cursor.execute(f"NOTIFY simple, '{i}'")
```
