import asyncio
import functools
import inspect
import pathlib
import ssl
from typing import Optional, Callable, Any, Awaitable

from aiohttp import web, WSMessage, WSMsgType
from jinja2 import FileSystemLoader
from aiohttp_asgi import ASGIResource

from .config import Config
from .exceptions import MokeiConfigError
from .request import Request
from .mokeiwebsocket import MokeiWebSocket, WebSocketRoute
from .logging import getLogger

logger = getLogger(__name__)

TemplateContext = dict[str, Any]


class Mokei:
    def __init__(
            self,
            config: Optional[Config] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        config = config or Config()
        self.config = config
        self._loop = loop or asyncio.new_event_loop()
        self._web_app: Optional[web.Application] = None
        self._asgi_resouces: list[ASGIResource] = []
        self._routes = web.RouteTableDef()
        self.view = self._routes.view
        self._handlers = {}
        self._ssl_context = None
        if config.template_dir and isinstance(config.template_dir, str):
            config.template_dir = pathlib.Path(config.template_dir)
        self._template_dir: Optional[pathlib.Path] = config.template_dir
        self._static_dirs: dict[str, pathlib.Path] = {}
        # list of zero-arg async functions that return None
        self._background_tasks: list[Callable[[], Awaitable[None]]] = []

    def _get_default_template_dir(self) -> Optional[pathlib.Path]:
        for dirname in ['template', 'templates']:
            templatedir = pathlib.Path(dirname)
            if templatedir.exists():
                return templatedir

    def _get_default_static_dir(self) -> Optional[pathlib.Path]:
        for dirname in ['static', 'public']:
            staticdir = pathlib.Path(dirname)
            if staticdir.exists():
                self.set_static_dir(f'/{dirname}', staticdir)

    def redirect(self, target):
        if isinstance(target, str):
            if target.startswith('/'):
                raise web.HTTPFound(target)
            else:
                if target in self._handlers:
                    raise web.HTTPFound(self._handlers[target])
        elif isinstance(target, Callable):
            if target.__name__ in self._handlers:
                raise web.HTTPFound(self._handlers[target.__name__])

    @staticmethod
    def _get_normalized_handler(raw_handler):
        if getattr(raw_handler, '_is_mokei_normalized', False):
            # this handler has been normalized already - return as is
            return raw_handler

        def first_param_is_request(hndlr) -> bool:
            sig = inspect.signature(hndlr)
            try:
                first_param = next(iter(sig.parameters.items()))
                # noinspection PyProtectedMember,PyUnresolvedReferences
                if (
                        first_param[0] == 'request' and first_param[1].annotation is inspect._empty
                        or first_param[1].annotation is Request):
                    return True
            except StopIteration:
                pass
            return False

        async def handler_with_request(_request: Request, *args, **kwargs):
            return await raw_handler(*args, **kwargs)

        if first_param_is_request(raw_handler):
            handler = raw_handler
        else:
            handler = handler_with_request

        # continue with parameters here?

        handler._is_mokei_normalized = True
        return handler

    def template(self, template_name: str):
        """Decorator method to decorate routes which use templates."""
        if not self.config.use_templates:
            raise MokeiConfigError('Set Config.use_templates to True to use templates')
        from aiohttp_jinja2 import template

        def decorator(handler):
            normalized_handler = self._get_normalized_handler(handler)
            return template(template_name)(normalized_handler)

        return decorator

    def _route_handler(self, path: str, http_method: str):
        """Generic decorator method for handling any http method
        Do not call this method directly, but use partialmethod to create methods for handling specific http methods
        """

        def decorator(handler):
            normalized_handler = self._get_normalized_handler(handler)
            getattr(self._routes, http_method)(path)(normalized_handler)
            self._handlers[handler.__name__] = path
            return handler

        return decorator

    get = functools.partialmethod(_route_handler, http_method='get')
    post = functools.partialmethod(_route_handler, http_method='post')
    head = functools.partialmethod(_route_handler, http_method='head')
    patch = functools.partialmethod(_route_handler, http_method='patch')
    delete = functools.partialmethod(_route_handler, http_method='delete')
    put = functools.partialmethod(_route_handler, http_method='put')

    def background_task(self, fn):
        self._background_tasks.append(fn)
        return fn

    def set_static_dir(self, prefix: str, dir_path: str | pathlib.Path) -> 'Mokei':
        if prefix not in self._static_dirs:
            self._static_dirs[prefix] = dir_path
            self._routes.static(prefix, dir_path)
        return self

    def set_template_dir(self, template_dir: str | pathlib.Path) -> 'Mokei':
        self.config.template_dir = pathlib.Path(template_dir)
        return self

    def load_cert(self, certfile, keyfile=None, password=None) -> 'Mokei':
        if self._ssl_context is None:
            self._ssl_context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH
            )
        self._ssl_context.load_cert_chain(certfile, keyfile, password)
        return self

    def register_asgi_app(self, asgi_app, path: str) -> None:
        self._asgi_resouces = ASGIResource(asgi_app, root_path=path)

    def run(
            self,
            config: Optional[Config] = None,
            **kwargs,
    ) -> None:
        """kwargs will be passed directly to Config
         See Config definition more information for acceptable kwarg names and types
        """
        if config is not None:
            self.config.__dict__.update(config.__dict__)
        for key, value in kwargs.items():
            setattr(self.config, key, value)
        # create aiohttp web application
        self._web_app = web.Application(middlewares=[])
        self._web_app.add_routes(self._routes)
        self._web_app['state'] = {}
        self._web_app.middlewares.extend(self.config.middlewares)

        # set up aiohttp_jinja2 templates, if necessary
        if self.config.use_templates:
            if self._template_dir is None:
                self._template_dir = self._get_default_template_dir()
            if self._template_dir is not None:
                from aiohttp_jinja2 import setup as template_setup
                template_setup(self._web_app, loader=FileSystemLoader(self._template_dir))

        # setup aiohttp_swagger documentation
        if self.config.use_swagger:
            from aiohttp_swagger import setup_swagger
            setup_swagger(self._web_app)

        # load certificates, if available
        if self.config.certfile is not None:
            self.load_cert(self.config.certfile, self.config.keyfile, self.config.password)

        # create background tasks
        for background_task in self._background_tasks:
            self._loop.create_task(background_task())

        # register any asgi resources
        for asgi_resource in self._asgi_resouces:
            self._web_app.router.register_resource(asgi_resource)

        # run the application
        web.run_app(self._web_app, host=self.config.host, port=self.config.port,
                    ssl_context=self._ssl_context, loop=self._loop)

    def websocketroute(self, path: str) -> WebSocketRoute:
        socket_route = WebSocketRoute(path)

        async def ws(request: Request) -> MokeiWebSocket:
            return await self._handle_websocket(request, socket_route)

        self._routes.get(path)(ws)
        return socket_route

    async def _handle_websocket(self, request: Request, socket_route: WebSocketRoute) -> MokeiWebSocket:
        """ Called when a new websocket connection is established from a client
        """
        ws = MokeiWebSocket(request)
        try:
            await ws.prepare(request)
        except ConnectionResetError:
            return ws
        socket_route.sockets.add(ws)
        # noinspection PyProtectedMember
        await socket_route._onconnect_handler(ws)
        async for msg in ws:
            msg: WSMessage
            if msg.type is WSMsgType.TEXT:
                # noinspection PyProtectedMember
                await socket_route._ontext_handler(ws, msg.data)
            elif msg.type is WSMsgType.BINARY:
                # noinspection PyProtectedMember
                await socket_route._onbinary_handler(ws, msg.data)

        socket_route.sockets.remove(ws)
        # noinspection PyProtectedMember
        await self._loop.create_task(socket_route._ondisconnect_handler(ws))
        return ws
