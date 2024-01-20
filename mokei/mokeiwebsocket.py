import asyncio
import collections
import json
from typing import Callable, Awaitable, Optional, Iterable
import uuid

from aiohttp import web

from .datatypes import JsonDict
from .request import Request
from .logging import getLogger

logger = getLogger(__name__)

# MokeiEventMarket, a marker prepended to json data in raw message when sending/receiving events (rather than text)
_MEM = 'μοκιε'


class MokeiWebSocket(web.WebSocketResponse):
    def __init__(self, request: Request, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = uuid.uuid4()
        self.request = request

    def __repr__(self):
        return f'<MokeiWebSocket {self.request.remote} {self.id}>'

    def __bool__(self):
        # Ensure that "if websocket:" works properly.
        # For some reason bool(inst_of_superclass) evaluates to False
        return True


_OnConnectHandler = Callable[[MokeiWebSocket], Awaitable[None]]
_OnDisconnectHandler = Callable[[MokeiWebSocket], Awaitable[None]]
_OnEventHandler = Callable[[MokeiWebSocket, JsonDict], Awaitable[None]]
_OnTextHandler = Callable[[MokeiWebSocket, str], Awaitable[None]]
_OnBinaryHandler = Callable[[MokeiWebSocket], Awaitable[None]]


class WebSocketRoute:
    def __init__(self, path):
        self.path = path
        self._onconnect_handlers: list[_OnConnectHandler] = []
        self._ondisconnect_handlers: list[_OnDisconnectHandler] = []
        self._ontext_handlers: list[_OnTextHandler] = []
        self._onbinary_handlers: list[_OnBinaryHandler] = []
        self._onevent_handlers: dict[str, list[_OnEventHandler]] = collections.defaultdict(list)
        self.sockets: set[MokeiWebSocket] = set()

    async def _onconnect_handler(self, ws: MokeiWebSocket) -> None:
        """Internal method called when a new websocket connection is received
        This method calls all handlers registered by ws.onconnect
        in the order that they were registered.
        """
        await asyncio.gather(*(handler(ws) for handler in self._onconnect_handlers))

    async def _ondisconnect_handler(self, ws: MokeiWebSocket) -> None:
        """Internal method called when a websocket disconnects
        This method calls all handlers registered by ws.disonconnect
        in the order that they were registered.
        """
        await asyncio.gather(*(handler(ws) for handler in self._ondisconnect_handlers))

    async def _ontext_handler(self, ws: MokeiWebSocket, message: str) -> None:
        """Internal method called when a websocket receives any text
        This method checks if the text is a Mokei Event (i.e. starts with _MEM)
        If it is an event, all self._onenvent_handlers are called in order
        If not, then all self._ontext_handlers are called in order
        """
        logger.debug(message)
        if message.startswith(_MEM):
            event_dict = json.loads(message[len(_MEM):])
            event = event_dict.get('event')
            data_dict = event_dict.get('data')
            if not event:
                return
            handlers = self._onevent_handlers.get(event)
            await asyncio.gather(*(handler(ws, data_dict) for handler in handlers))
        else:
            await asyncio.gather(*(handler(ws, message) for handler in self._ontext_handlers))

    async def _onbinary_handler(self, ws: MokeiWebSocket, message: bytes) -> None:
        """Internal method called when a websocket receives any binary
        """
        await asyncio.gather(*(handler(ws, message) for handler in self._onbinary_handlers))

    def onconnect(self, handler):
        """Decorator for async functions to be run when a new websocket connection is received

        @yourwebsocketroute.onconnect
        async def send_welcome_message(websocket: Websocket):
            logger.info(f'New connection from {websocket.remote}'
            await websocket.send_text('Welcome!')
        """
        self._onconnect_handlers.append(handler)
        return handler

    def ondisconnect(self, handler):
        """Decorator for async functions to be run when a websocket connection is closed

        @yourwebsocketroute.ondisconnect
        async def send_welcome_message(websocket: Websocket):
            logger.info('Websocket from %s disconnected', websocket.remote)
        """
        self._ondisconnect_handlers.append(handler)
        return handler

    def on(self, event: str) -> Callable[[_OnEventHandler], _OnEventHandler]:
        """Decorator for mokei events

        @yourwebsocketroute.on('my_event')
        async def log_event(websocket: Websocket, data: JsonData):
            logger.info('Received my_event')
            logger.info(data)
        """
        def decorator(handler: _OnEventHandler) -> _OnEventHandler:
            self._onevent_handlers[event].append(handler)
            return handler

        return decorator

    def ontext(self, handler: _OnTextHandler) -> _OnTextHandler:
        self._ontext_handlers.append(handler)
        return handler

    def onbinary(self, handler: _OnBinaryHandler) -> _OnBinaryHandler:
        self._onbinary_handlers.append(handler)
        return handler

    async def send_text(self, message: str, *target: MokeiWebSocket,
                        exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:
        # handle cases where exclude is None or a single MokeiWebSocket
        exclude = exclude or ()

        if isinstance(exclude, MokeiWebSocket):
            # harmonize arg "exclude" always to be Iterable[MokeiWebSocket]
            exclude = (exclude,)

        # create a list of sockets to be removed (for failure) post-send
        remove_sockets = list()

        # create a set of recipient sockets (target is just an Iterable[WebSocket] at this point)
        if target:
            recipient_sockets = {target_socket for target_socket in target}
        else:
            # target all sockets in self by default, unless specifically provided in args
            recipient_sockets = {target_socket for target_socket in self.sockets}

        # remove from recipient_sockets any sockets listed in exclude (affects this one event only)
        for socket_to_remove in exclude:
            if socket_to_remove in recipient_sockets:
                recipient_sockets.remove(socket_to_remove)

        async def send_to_single_ws(_message: str, _ws: MokeiWebSocket):
            """Send text to a single websocket
            """
            try:
                await _ws.send_str(_message)
            except ConnectionResetError:
                # unexpected disconnect from remote side
                remove_sockets.append(_ws)
                if _ws in self.sockets:
                    self.sockets.remove(_ws)

        # send the event
        await asyncio.gather(*(send_to_single_ws(message, recipient_socket) for recipient_socket in recipient_sockets))

        # remove any failed sockets from this route
        for socket_to_remove in remove_sockets:
            if socket_to_remove in self.sockets:
                self.sockets.remove(socket_to_remove)

    async def send_event(self, event: str, data: dict, *target: MokeiWebSocket,
                         exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:

        message = _MEM + json.dumps({'event': event, 'data': data})

        await self.send_text(message, *target, exclude=exclude)

    def __repr__(self):
        return f'<WebSocketRoute {self.path}>'
