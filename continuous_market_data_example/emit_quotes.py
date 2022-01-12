"""
This script is meant to simulate live market quotes in a supplied
ticker. There will be a random sleep interval between quotes.
Usage:
python3 emit_quotes.py AAPL MSFT SPY
This continuously emits random stock moves in those names until exited
"""

import asyncio
import random

import aio_pika
import sys
import numpy as np


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


SYMBOLS = {
    "AAPL": {
        "mean": 145.0,
        "std": 3.0
    },
    "MSFT": {
        "mean": 123.0,
        "std": 2.0
    },
    "TSLA": {
        "mean": 223.0,
        "std": 7.0
    }
}


class SymbolNotSupportedError(Exception):
    pass


def get_price_of_quote(mean, std):
    px = float(np.random.normal(mean, std, size=1)[0])
    return px


def create_quote(ticker: str) -> float:
    if ticker not in SYMBOLS:
        raise SymbolNotSupportedError()

    mean, std = SYMBOLS[ticker]['mean'], SYMBOLS[ticker]['std']
    price = get_price_of_quote(mean, std)
    return price


async def main(loop):
    try:
        connection = await aio_pika.connect_robust(loop=loop)
    except ConnectionError:
        print(COLORS.FAIL + f"RabbitMQ host is not up! Haulting consumer process")
        return 127

    symbols = sys.argv[1:]
    async with connection:
        channel = await connection.channel()
        while True:
            symbol = random.choice(symbols)
            print(COLORS.OKBLUE + f"Creating quote for {symbol}")
            try:
                quote_price = create_quote(symbol)
                print(COLORS.OKCYAN + f"{symbol} quoted at {quote_price}")
            except SymbolNotSupportedError:
                print(COLORS.FAIL + f"{symbol} not supported!")

            await asyncio.sleep(np.random.randint(low=0, high=2))
            quote = aio_pika.Message(
                body=f"{symbol} @ {quote_price}".encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            await channel.default_exchange.publish(quote, routing_key=symbol)


if __name__ == "__main__":
    print(COLORS.OKGREEN + f"Starting quote simulator... To exit press CTRL+C")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()






