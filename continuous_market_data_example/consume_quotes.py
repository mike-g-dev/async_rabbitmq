import asyncio
import aio_pika
import sys


class COLORS:
    NORMAL = '\033[0m'
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


SUPPORTED_SYMBOLS = ['TSLA', 'AAPL', 'MSFT']


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process(requeue=True):
        print(COLORS.OKBLUE + str(message.body))


async def main(loop):
    try:
        connection = await aio_pika.connect_robust(
            loop=loop,
            client_properties={"connection_name": "Consumer connection"}
        )
    except ConnectionError:
        print(COLORS.FAIL + f"RabbitMQ host is not up! Haulting consumer process")
        sys.exit(127)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    for symbol in SUPPORTED_SYMBOLS:
        queue = await channel.declare_queue(symbol, durable=True)
        await queue.consume(process_message)


if __name__ == "__main__":
    print(COLORS.OKGREEN + f"Quote consumer waiting for quotes... To exit press CTRL+C")
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()
