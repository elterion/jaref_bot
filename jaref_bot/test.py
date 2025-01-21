import asyncio
import websockets
from datetime import datetime
import json

async def test_fun():
    url = 'wss://ws.okx.com:8443/ws/v5/public'
    async with websockets.connect(url, ping_interval=10, ping_timeout=20) as ws:
        print(f'Connected {datetime.now().isoformat()}')
        subs = dict(
                op = 'subscribe',
                args = [dict(channel='mark-price', instId='BTC-USDT')]
            )
        await ws.send(json.dumps(subs))

        async for msg in ws:
            print(msg)

    print(f'Disconnected {datetime.now().isoformat()}')


if __name__ == '__main__':
    print('starting...')
    asyncio.run(test_fun())