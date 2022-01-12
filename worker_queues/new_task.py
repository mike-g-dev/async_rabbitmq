import sys
import aio_pika
import asyncio
from colors import COLORS


async def create_connection(event_loop):
    return await aio_pika.connect(loop=event_loop)


def create_message(args):
    message_body = b" ".join(ch.encode() for ch in args) or b"Default task"
    return aio_pika.Message(
        message_body,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )


async def main(loop):
    connection = await create_connection(event_loop=loop)
    async with connection as conn:
        channel = await conn.channel()

        message = create_message(args=sys.argv[1:])
        await channel.default_exchange.publish(
            message,
            routing_key="task-queue"
        )
        print(COLORS.OKGREEN + f" [x] Sent {message.body}")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
