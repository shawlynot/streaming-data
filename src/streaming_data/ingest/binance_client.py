from binance import AsyncClient, BinanceSocketManager


async def kline_listener(client):
    bm = BinanceSocketManager(client)
    async with bm.kline_socket(symbol='BNBBTC', interval=AsyncClient.KLINE_INTERVAL_1SECOND) as stream:
        while True:
            res = await stream.recv()
            print(res)

async def main():
    client = await AsyncClient.create()
    await kline_listener(client)