import sys
import asyncio
import aio_pika


async def main(loop):
    connection = await aio_pika.connect(loop=loop)

    async with connection:
        channel = await connection.channel()

        logs_exchange = await channel.declare_exchange(
            "topic_logs", aio_pika.ExchangeType.TOPIC
        )

        message_body = b" ".join(
            arg.encode() for arg in sys.argv[2:]) or b"Default message"

        message = aio_pika.Message(
            message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )

        routing_key = sys.argv[1] if len(sys.argv) > 2 else "annonymous.info"
        await logs_exchange.publish(message, routing_key=routing_key)

        print(" [x] Sent %r" % message.body)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))