import asyncio
import collections
import functools
import inspect
import json
import uuid
from typing import Callable, Awaitable, Optional, Iterable
from logging import getLogger

import pydantic
from aiohttp import web

from .datatypes import JsonDict, JsonArray
from .request import Request

logger = getLogger(__name__)

# MokeiEventMarket, a marker prepended to json data in raw message when sending/receiving events (rather than text)
_MEM = 'μοκιε'


class MokeiWebSocket(web.WebSocketResponse):
    def __init__(self, request: Request, route: 'MokeiWebSocketRoute', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.id = uuid.uuid4()
        self.request = request
        self._route = route

    def __repr__(self) -> str:
        return f'<MokeiWebSocket {self.request.remote} {self.id}>'

    def __bool__(self) -> bool:
        # Ensure that "if websocket:" works properly.
        # For some reason bool(inst_of_superclass) evaluates to False
        return True

    async def send_text(self, message: str) -> None:
        await self._route.send_text(message, self)

    async def send_event(self, event: str, data: JsonDict) -> None:
        await self._route.send_event(event, data, self)

    async def send_model(self, model: pydantic.BaseModel) -> None:
        message = model.model_dump_json()
        await self.send_text(message)

    # noinspection PyMethodOverriding
    async def send_json(self, data: JsonDict | JsonArray) -> None:
        message = json.dumps(data)
        await self.send_text(message)

    async def send(self, obj: JsonDict | JsonArray | str | pydantic.BaseModel) -> None:
        if isinstance(obj, dict) or isinstance(obj, list):
            await self.send_json(obj)
        elif isinstance(obj, str):
            await self.send_text(obj)
        elif isinstance(obj, pydantic.BaseModel):
            await self.send_model(obj)


OnConnectHandler = Callable[[MokeiWebSocket], Awaitable[None]]
OnDisconnectHandler = Callable[[MokeiWebSocket], Awaitable[None]]
OnEventHandler = Callable[[MokeiWebSocket, JsonDict], Awaitable[None]]
OnTextHandler = Callable[[MokeiWebSocket, str], Awaitable[None]]
OnBinaryHandler = Callable[[MokeiWebSocket, bytes], Awaitable[None]]


class MokeiWebSocketRoute:
    def __init__(self, path) -> None:
        self.path = path
        self._onconnect_handlers: list[OnConnectHandler] = []
        self._ondisconnect_handlers: list[OnDisconnectHandler] = []
        self._ontext_handlers: list[OnTextHandler] = []
        self._onbinary_handlers: list[OnBinaryHandler] = []
        self._onevent_handlers: dict[str, list[OnEventHandler]] = collections.defaultdict(list)
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

    @staticmethod
    def _get_normalized_handler(raw_handler):
        """Allow MokeiWebsocketRoute handlers to be defined with or without the first "websocket" parameter"""
        if not asyncio.iscoroutinefunction(raw_handler):
            raise TypeError('handler must be an async function')

        if getattr(raw_handler, '_is_mokei_normalized', False):
            # this handler has been normalized already - return as is
            return raw_handler

        sig = inspect.signature(raw_handler)
        params = sig.parameters

        def first_param_is_ws() -> bool:
            for param_name, param in params.items():
                if ((param_name in ('ws', 'websocket') and param.annotation is param.empty)
                        or param.annotation is MokeiWebSocket):
                    return True
            return False

        @functools.wraps(raw_handler)
        async def normalized_handler_with_ws(_ws: MokeiWebSocket, *args, **kwargs) -> None:
            return await raw_handler(*args, **kwargs)

        if first_param_is_ws():
            handler = raw_handler
        else:
            handler = normalized_handler_with_ws

        handler._is_mokei_normalized = True
        return handler

    def onconnect(self, handler):
        """Decorator for async functions to be run when a new websocket connection is received

        @yourwebsocketroute.onconnect
        async def send_welcome_message(websocket: MokeiWebsocket):
            logger.info(f'New connection from {websocket.request.remote}'
            await websocket.send_text('Welcome!')
        """
        handler = self._get_normalized_handler(handler)
        self._onconnect_handlers.append(handler)
        return handler

    def ondisconnect(self, handler):
        """Decorator for async functions to be run when a websocket connection is closed

        @yourwebsocketroute.ondisconnect
        async def send_welcome_message(websocket: MokeiWebsocket):
            logger.info('Websocket from %s disconnected', websocket.request.remote)
        """
        handler = self._get_normalized_handler(handler)
        self._ondisconnect_handlers.append(handler)
        return handler

    def on(self, event: str) -> Callable[[OnEventHandler], OnEventHandler]:
        """Decorator for mokei events

        @yourwebsocketroute.on('my_event')
        async def log_event(websocket: Websocket, data: JsonData):
            logger.info('Received my_event')
            logger.info(data)
        """

        def decorator(handler: OnEventHandler) -> OnEventHandler:
            handler = self._get_normalized_handler(handler)
            self._onevent_handlers[event].append(handler)
            return handler

        return decorator

    def ontext(self, handler: OnTextHandler) -> OnTextHandler:
        handler = self._get_normalized_handler(handler)
        self._ontext_handlers.append(handler)
        return handler

    def onbinary(self, handler: OnBinaryHandler) -> OnBinaryHandler:
        handler = self._get_normalized_handler(handler)
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

    async def send(self, obj: JsonDict | JsonArray | str | pydantic.BaseModel, *target: MokeiWebSocket,
                   exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:
        if isinstance(obj, dict) or isinstance(obj, list):
            await self.send_json(obj, *target, exclude=exclude)
        elif isinstance(obj, str):
            await self.send_text(obj, *target, exclude=exclude)
        elif isinstance(obj, pydantic.BaseModel):
            await self.send_model(obj, *target, exclude=exclude)

    async def send_json(self, data: JsonDict | JsonArray, *target: MokeiWebSocket,
                        exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:
        message = json.dumps(data)
        await self.send_text(message, *target, exclude=exclude)

    async def send_model(self, model: pydantic.BaseModel, *target: MokeiWebSocket,
                         exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:
        message = model.model_dump_json()
        await self.send_text(message, *target, exclude=exclude)

    async def send_event(self, event: str, data: JsonDict | JsonArray | pydantic.BaseModel, *target: MokeiWebSocket,
                         exclude: Optional[MokeiWebSocket | Iterable[MokeiWebSocket]] = None) -> None:
        if isinstance(event, pydantic.BaseModel):
            message = _MEM + json.dumps({'event': event, 'data': data.model_dump(mode='json')})
        else:
            message = _MEM + json.dumps({'event': event, 'data': data})

        await self.send_text(message, *target, exclude=exclude)

    def __repr__(self):
        return f'<WebSocketRoute {self.path}>'
