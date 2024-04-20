import asyncio
import collections
import json
import random

import aiohttp
from aiohttp import WSMessage

_MEM = 'μοκιε'


class MokeiWebSocketClient:
    def __init__(self, url: str):
        self.url = url
        self._ws = None
        self._default_backoff = 1.0
        self._current_backoff = self._default_backoff
        self._max_backoff = 15.0
        self._unsent_messages = collections.deque()
        self._onconnect_handlers = []
        self._ontext_handlers = []
        self._ondisconnect_handlers = []
        self._handlers: dict[str, list] = collections.defaultdict(list)
        self._session: aiohttp.ClientSession | None = None

    def _get_backoff(self):
        backoff = self._current_backoff
        self._current_backoff += self._current_backoff * random.random()
        self._current_backoff = min(self._current_backoff, self._max_backoff)
        return backoff

    def _reset_backoff(self):
        self._current_backoff = self._default_backoff

    async def _onconnect_handler(self, ws):
        await asyncio.gather(*(handler(ws) for handler in self._onconnect_handlers))

    async def _ondisconnect_handler(self, ws):
        await asyncio.gather(*(handler(ws) for handler in self._ondisconnect_handlers))

    async def _ontext_handler(self, ws, msg: str):
        if msg.startswith(_MEM):
            event_data = json.loads(msg)
            if 'event' not in event_data or 'data' not in event_data:
                return
            event = event_data['event']
            data = event_data['data']
            await asyncio.gather(*[handler(ws, data) for handler in self._handlers[event]])
        await asyncio.gather(*[handler(ws, msg) for handler in self._ontext_handlers])

    def onconnect(self, handler):
        """Decorator method.

        Decorate an async function which accepts one argument (a mokei.Websocket), and returns None

        Example:

        client = MokeiWebSocketClient('https://someurl.com')

        @client.onconnect
        async def connectionhandler(socket: mokei.WebSocket) -> None:
            logger.info(f'New connection from {socket.request.remote}')
        """
        self._onconnect_handlers.append(handler)
        return handler

    def ondisconnect(self, handler):
        """Decorator method.

        Decorate an async function which accepts one argument (a mokei.Websocket), and returns None

        Example:

        client = MokeiWebSocketClient('https://someurl.com')

        @client.ondisconnect
        async def disconnecthandler(socket: mokei.WebSocket) -> None:
            logger.info(f'Lost connection to {socket.request.remote}')
        """
        self._ondisconnect_handlers.append(handler)
        return handler

    async def connect(self):
        self._session = aiohttp.ClientSession()
        async with self._session as session:
            while True:
                try:
                    async with session.ws_connect(self.url) as ws:
                        self._reset_backoff()
                        self._ws = ws
                        await self._onconnect_handler(ws)
                        await self._send_unsent_messages()
                        async for msg in ws:
                            msg: WSMessage
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._ontext_handler(ws, msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
                except aiohttp.ClientConnectorError:
                    pass
                except RuntimeError:
                    raise RuntimeError

                if self._ws:
                    await self._ondisconnect_handler(self._ws)
                self._ws = None
                await asyncio.sleep(self._get_backoff())

    async def close(self):
        await self._session.close()

    async def _send_unsent_messages(self):
        while self._unsent_messages:
            try:
                if not self._ws:
                    break
                await self._ws.send_str(self._unsent_messages[0])
                self._unsent_messages.popleft()
            except ConnectionResetError:
                break

    async def send(self, text: str):
        self._unsent_messages.append(text)
        await self._send_unsent_messages()

    def ontext(self, handler):
        self._ontext_handlers.append(handler)
        return handler

    def on(self, event: str):
        def decorator(fn):
            self._handlers[event].append(fn)
            return fn

        return decorator
