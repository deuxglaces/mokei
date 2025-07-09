import asyncio
import collections
import dataclasses
import json
import random
import time
from typing import Optional

import aiohttp
from aiohttp import WSMessage

_MEM = 'μοκιε'


@dataclasses.dataclass
class MokeiWebSocketClientConfig:
    heartbeat: float = 0.0
    autopong: bool = True
    default_backoff: float = 1.0
    max_backoff: float = 15.0
    automatic_reconnect: bool = True



class MokeiWebSocketClient:
    def __init__(self, url: str, config: Optional[MokeiWebSocketClientConfig] = None, **kwargs):
        config = config or MokeiWebSocketClientConfig(**kwargs)
        self.url = url
        self.heartbeat = config.heartbeat
        self.automatic_reconnect = config.automatic_reconnect
        self._first_connect = True
        self._connected = False
        self._ws = None
        self._default_backoff = config.default_backoff
        self._current_backoff = self._default_backoff
        self._max_backoff = config.max_backoff
        self._unsent_messages = collections.deque()
        self._unsent_binary = collections.deque()
        self._unsent_pings = 0
        self._unsent_pongs = 0
        self._onconnect_handlers = []
        self._ontext_handlers = []
        self._onbinary_handlers = []
        self._onerror_handlers = []
        self._ondisconnect_handlers = []
        self._onping_handlers = []
        self._onpong_handlers = []
        self._handlers: dict[str, list] = collections.defaultdict(list)
        self._session: aiohttp.ClientSession | None = None
        self._last_msg: float = time.time()
        if config.autopong:
            self.onping(self.pong)

    @property
    def is_connected(self):
        return self._connected

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

    async def _onping_handler(self):
        await asyncio.gather(*[handler() for handler in self._onping_handlers])

    async def _onpong_handler(self):
        self._last_msg = time.time()
        await asyncio.gather(*[handler() for handler in self._onpong_handlers])

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

    def onping(self, handler):
        self._onping_handlers.append(handler)
        return handler

    def onpong(self, handler):
        self._onpong_handlers.append(handler)

    async def _send_heartbeat(self, interval: float):
        while True:
            if self._connected and time.time() - self._last_msg > interval * 2.0:
                await self._ws.close()
            await asyncio.sleep(interval)
            if self.is_connected:
                await self.ping()

    async def connect(self, **kwargs):
        if self.heartbeat:
            asyncio.get_event_loop().create_task(self._send_heartbeat(self.heartbeat))
        self._session: aiohttp.ClientSession = aiohttp.ClientSession()
        async with self._session as session:
            while self._first_connect or self.automatic_reconnect:
                try:
                    async with session.ws_connect(self.url, **kwargs, autoping=False) as ws:
                        self._connected = True
                        self._last_msg = time.time()
                        self._reset_backoff()
                        self._ws = ws
                        await self._onconnect_handler()
                        await self._send_unsent_messages()
                        async for msg in ws:
                            self._last_msg = time.time()
                            msg: WSMessage
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._ontext_handler(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await self._onbinary_handler(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                await self._onerror_handler(msg.data)
                            elif msg.type == aiohttp.WSMsgType.PING:
                                await self._onping_handler()
                            elif msg.type == aiohttp.WSMsgType.PONG:
                                await self._onpong_handler()
                            elif msg.type == aiohttp.WSMsgType.CLOSE:
                                pass
                            elif msg.type == aiohttp.WSMsgType.CLOSING:
                                pass
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                pass

                except aiohttp.ClientError:
                    pass

                self._connected = False
                if self._ws:
                    await self._ondisconnect_handler()
                self._ws = None
                await asyncio.sleep(self._get_backoff())

    async def reset(self):
        """Close the connection and restart"""
        self._first_connect = True
        await self._ws.close()

    async def close(self) -> None:
        """Close the connection and shut down"""
        self.automatic_reconnect = False
        await self._ws.close()

    async def _send_unsent_messages(self):
        while self._unsent_messages:
            try:
                if not self._ws:
                    break
                await self._ws.send_str(self._unsent_messages[0])
                self._unsent_messages.popleft()
            except ConnectionResetError:
                break

    async def _send_unsent_pings(self):
        while self._unsent_pings > 0:
            try:
                if not self._ws:
                    break
                await self._ws.ping()
                self._unsent_pings -= 1
            except ConnectionResetError:
                break

    async def _send_unsent_pongs(self):
        while self._unsent_pongs > 0:
            try:
                if not self._ws:
                    break
                await self._ws.pong()
                self._unsent_pongs -= 1
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

    async def ping(self) -> None:
        self._unsent_pings += 1
        await self._send_unsent_pings()

    async def pong(self) -> None:
        self._unsent_pongs += 1
        await self._send_unsent_pongs()

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
