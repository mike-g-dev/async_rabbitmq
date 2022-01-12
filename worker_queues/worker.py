import asyncio
import aio_pika
from colors import COLORS

loop = asyncio.get_event_loop()


async def create_connection(event_loop):
    return await aio_pika.connect(loop=event_loop)


async def on_message(message):
    async with message.process():
        print(COLORS.OKGREEN + " [x] Received message %r" % message.body)


async def main(loop):
    connection = await create_connection(event_loop=loop)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(
        "task-queue",
        durable=True
    )
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))

    print(COLORS.OKGREEN + " [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
