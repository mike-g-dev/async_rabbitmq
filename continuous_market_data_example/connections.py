import aio_pika
import asyncio


async def get_connection():
    return await aio_pika.connect_robust()


def create_connection_pool(loop, conn_fn=get_connection, max_size=1):
    return aio_pika.pool.Pool(conn_fn, max_size=max_size, loop=loop)


async def get_channel(conn_pool):
    async with conn_pool.aquire() as conn:
        return await conn.channel()


def create_channel_pool(loop, channel_fn, max_size=1):
    return aio_pika.pool.Pool(channel_fn, max_size=max_size, loop=loop)




