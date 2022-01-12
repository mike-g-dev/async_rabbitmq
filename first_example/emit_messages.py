import asyncio
import aio_pika
import sys
from colors import COLORS


async def main(loop):
    print(COLORS.OKBLUE + f"[*] Starting RabbitMQ producer...")
    conn = await aio_pika.connect_robust(loop=loop)
    print(COLORS.OKBLUE + f"[*] Valid connection to broker established")

    message = sys.argv[1]
    async with conn:
        routing_key = "test-queue"
        print(COLORS.OKBLUE + f"[*] Publishing {message} to {routing_key}")
        channel = await conn.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=routing_key
        )
    print(COLORS.OKBLUE + f"[x] Done publishing")


if __name__ == "__main__":
    print(COLORS.OKGREEN + f"[*] Starting event loop")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
    print(COLORS.OKGREEN + f"[x] Event loop exited without error")



