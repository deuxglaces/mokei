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
        self._unsent_binary = collections.deque()
        self._onconnect_handlers = []
        self._ontext_handlers = []
        self._onbinary_handlers = []
        self._onerror_handlers = []
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

    async def _onconnect_handler(self):
        await asyncio.gather(*(handler() for handler in self._onconnect_handlers))

    async def _ondisconnect_handler(self):
        await asyncio.gather(*(handler() for handler in self._ondisconnect_handlers))

    async def _ontext_handler(self, msg: str):
        if msg.startswith(_MEM):
            event_data = json.loads(msg[len(_MEM):])
            if 'event' not in event_data or 'data' not in event_data:
                return
            event = event_data['event']
            data = event_data['data']
            await asyncio.gather(*[handler(data) for handler in self._handlers[event]])
        else:
            await asyncio.gather(*[handler(msg) for handler in self._ontext_handlers])

    async def _onbinary_handler(self, msg: bytes):
        await asyncio.gather(*[handler(msg) for handler in self._onbinary_handlers])

    async def _onerror_handler(self, data):
        await asyncio.gather(*[handler(data) for handler in self._onerror_handlers])

    def onconnect(self, handler):
        """Decorator method.

        Decorate an async function which accepts one argument (a mokei.Websocket), and returns None

        Example:

        client = MokeiWebSocketClient('https://someurl.com')

        @client.onconnect
        async def connectionhandler() -> None:
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
        async def disconnecthandler() -> None:
            logger.info(f'Lost connection to {socket.request.remote}')
        """
        self._ondisconnect_handlers.append(handler)
        return handler

    async def connect(self, **kwargs):
        self._session = aiohttp.ClientSession()
        async with self._session as session:
            while True:
                try:
                    async with session.ws_connect(self.url, **kwargs) as ws:
                        self._reset_backoff()
                        self._ws = ws
                        await self._onconnect_handler()
                        await self._send_unsent_messages()
                        async for msg in ws:
                            msg: WSMessage
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._ontext_handler(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await self._onbinary_handler(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                await self._onerror_handler(msg.data)
                except aiohttp.ClientError:
                    pass

                if self._ws:
                    await self._ondisconnect_handler()
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

    async def _send_unsent_binary(self):
        while self._unsent_binary:
            try:
                if not self._ws:
                    break
                await self._ws.send_bytes(self._unsent_binary[0])
                self._unsent_binary.popleft()
            except ConnectionResetError:
                break

    async def send_text(self, text: str):
        self._unsent_messages.append(text)
        await self._send_unsent_messages()

    async def send_binary(self, data: bytes):
        self._unsent_binary.append(data)
        await self._send_unsent_binary()

    def ontext(self, handler):
        self._ontext_handlers.append(handler)
        return handler

    def onbinary(self, handler):
        self._onbinary_handlers.append(handler)
        return handler

    def onerror(self, handler):
        self._onerror_handlers.append(handler)
        return handler

    def on(self, event: str):
        def decorator(fn):
            self._handlers[event].append(fn)
            return fn

        return decorator
