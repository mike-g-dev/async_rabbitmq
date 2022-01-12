import asyncio
import sys
from aio_pika import connect, IncomingMessage, ExchangeType


async def on_message(message: IncomingMessage):
    async with message.process():
        print(" [x] %r:%r" % (message.routing_key, message.body))


async def main():
    print(f"Getting connection to rabbitmq...")
    connection = await connect(loop=loop)
    print(f"Connection to rabbitmq established.")

    print(f"Creating channel...")
    channel = await connection.channel()
    print(f"Channel created.")

    await channel.set_qos(prefetch_count=1)

    print(f"Creating topic_logs exchange...")
    logs_exchange = await channel.declare_exchange(
        "topic_logs",
        ExchangeType.TOPIC
    )
    print(f"topic_logs exchange created.")

    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write(f"Usage: {sys.argv[0]} [binding_key]...\n")
        sys.exit(1)

    print(f"Declaring queue...")
    queue = await channel.declare_queue(durable=True)
    print(f"Queue declared.")

    print(f"Binding keys to exchange...")
    for binding_key in binding_keys:
        await queue.bind(logs_exchange, routing_key=binding_key)
        print(f"{binding_key} bound to exchange")

    print(f"Preparing to consume messages...")
    await queue.consume(on_message)
    print(f"After consumer.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(" [*] Waiting for logs. To exit press CTRL+C")
    loop.run_forever()