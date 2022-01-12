import asyncio
import aio_pika
from colors import COLORS


async def main(loop):
    print(COLORS.OKBLUE + f"[*] Starting RabbitMQ consumer...")
    connection = await aio_pika.connect_robust(
        loop=loop
    )
    print(COLORS.OKBLUE + f"[*] Valid connection to broker established")

    queue_name = "test-queue"
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(message.body)

                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    print(COLORS.OKGREEN + f"[*] Starting event loop")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
    print(COLORS.OKGREEN + f"[x] Event loop exited without error")