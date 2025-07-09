import asyncio
import functools
import inspect
import pathlib
import ssl
from typing import Optional, Callable, Any, Awaitable
from logging import getLogger

from aiohttp import web, WSMessage, WSMsgType
from jinja2 import FileSystemLoader
from aiohttp_asgi import ASGIResource

from .config import MokeiConfig
from .exceptions import MokeiConfigError
from ._middlewares import mokei_resp_type_middleware, convert_to_mokei_request
from .request import Request
from .websocket import MokeiWebSocket, MokeiWebSocketRoute

logger = getLogger(__name__)

TemplateContext = dict[str, Any]


class Mokei:
    def __init__(
            self,
            config: Optional[MokeiConfig] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        config = config or MokeiConfig()
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
        self._startup_tasks: list[Callable[[], Awaitable[None]]] = []
        self._shutdown_tasks: list[Callable[[], Awaitable[None]]] = []
        self._middlewares = []

    @staticmethod
    def _get_default_template_dir() -> Optional[pathlib.Path]:
        """Return the default template directory
        if there is a directory called "templates"
            then return the pathlib.Path object for that directory
        otherwise, return None
        """
        for dirname in ['template', 'templates']:
            templatedir = pathlib.Path(dirname)
            if templatedir.exists() and templatedir.is_dir():
                return templatedir

    @staticmethod
    def _get_default_static_dir() -> Optional[pathlib.Path]:
        """Return the default static directory
        If there is a directory called "static" or "public",
            then return the pathlib.Path object for that directory
        otherwise, return None
        """
        for dirname in ['static', 'public']:
            staticdir = pathlib.Path(dirname)
            if staticdir.exists():
                return staticdir

    def middleware(self, fn):
        """Decorator for functions to be registered as middleware. E.g.:
        app = Mokei()

        @app.middleware
        async def my_middleware(request, handler):
            request = do_something_to_modify_request(request)
            do_something_else()
            response = await handler(request)
            response = do_something_to_modify_response(response)
        """
        fn = web.middleware(fn)
        self._middlewares.append(fn)

    def redirect(self, target):
        # noinspection GrazieInspection
        """Method to send a redirect, rather than a response. E.g.:
                app = Mokei()

                @app.get('/old_endpoint')
                async def redirect_to_new_endpoint():
                    app.redirect('/new_endpoint')
                """
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
        """Allow Mokei handlers to be defined with or without the first "request" parameter"""
        if not asyncio.iscoroutinefunction(raw_handler):
            raise TypeError('handler must be an async function')

        if getattr(raw_handler, '_is_mokei_normalized', False):
            # this handler has been normalized already - return as is
            return raw_handler

        sig = inspect.signature(raw_handler)
        params = sig.parameters

        def first_param_is_request() -> bool:
            for param_name, param in params.items():
                if (param_name == 'request' and param.annotation is param.empty) or param.annotation is Request:
                    return True
            return False

        @functools.wraps(raw_handler)
        async def normalized_handler(r: Request):
            kwargs = {key: r.match_info[key] for key in r.match_info if key in params}
            return await raw_handler(r, **kwargs)

        @functools.wraps(raw_handler)
        async def normalized_handler_with_request(r: Request):
            kwargs = {key: r.match_info[key] for key in r.match_info if key in params}
            return await raw_handler(**kwargs)

        if first_param_is_request():
            handler = normalized_handler
        else:
            handler = normalized_handler_with_request

        handler._is_mokei_normalized = True
        return handler

    def template(self, template_name: str):
        """Decorator method to decorate routes which use templates.

        The handler function should return a dict containing the Jinja2 context
        E.g.:
        @app.get('/')
        @app.template('index.html') # this should exist in the templates directory
        async def serve_index():
            return {'context1': 'value1', 'context2': 'value2'}
        """
        if not self.config.use_templates:
            raise MokeiConfigError('Set Config.use_templates to True to use templates')
        from aiohttp_jinja2 import template

        def decorator(handler):
            normalized_handler = self._get_normalized_handler(handler)
            return template(template_name)(normalized_handler)

        return decorator

    def _route_handler(self, path: str, http_method: str):
        """Generic decorator method for handling any http method
        Do not call this method directly. Use partialmethod to create methods for handling specific http methods
        """

        def decorator(handler):
            if not asyncio.iscoroutinefunction(handler):
                raise TypeError('Handler must be a async function')
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
        """Decorator method to register background tasks. E.g.,
        app = Mokei()

        @app.background_task
        async def do_something_every_24_hours():
            while True:
                await asyncio.sleep(86400.0)
                await do_something()
        """
        self._background_tasks.append(fn)
        return fn

    def startup_task(self, fn):
        self._startup_tasks.append(fn)
        return fn

    def shutdown_task(self, fn):
        self._shutdown_tasks.append(fn)
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

    @staticmethod
    async def _run_tasks(task_list):
        await asyncio.gather(*(task() for task in task_list))

    def run(
            self,
            config: Optional[MokeiConfig] = None,
            **kwargs,
    ) -> None:
        """kwargs will be passed directly to Config
         See Config definition more information for acceptable kwarg names and types
        """
        if config is not None:
            self.config.__dict__.update(config.__dict__)
        for key, value in kwargs.items():
            setattr(self.config, key, value)
        if static_dir := self._get_default_static_dir():
            self.set_static_dir(f'/{static_dir.name}', static_dir)

        # create aiohttp web application
        self._web_app = web.Application(middlewares=[])
        self._web_app.add_routes(self._routes)
        self._web_app['state'] = {}
        self._web_app.middlewares.append(convert_to_mokei_request)
        self._web_app.middlewares.extend(self.config.middlewares)
        self._web_app.middlewares.extend(self._middlewares)
        self._web_app.middlewares.append(mokei_resp_type_middleware)

        # set up aiohttp_jinja2 templates, if necessary
        if self.config.use_templates:
            if self._template_dir is None:
                self._template_dir = self._get_default_template_dir()
            if self._template_dir is not None:
                from aiohttp_jinja2 import setup as template_setup
                template_setup(self._web_app, loader=FileSystemLoader(self._template_dir))

        # load certificates, if available
        if self.config.certfile is not None:
            self.load_cert(self.config.certfile, self.config.keyfile, self.config.password)

        # create background tasks
        for background_task in self._background_tasks:
            self._loop.create_task(background_task())

        # register any asgi resources
        for asgi_resource in self._asgi_resouces:
            self._web_app.router.register_resource(asgi_resource)

        # setup aiohttp_swagger documentation
        if self.config.use_swagger:
            from aiohttp_swagger import setup_swagger
            ss_kwargs = {}
            if self.config.swagger_api_base_url:
                ss_kwargs['api_base_url'] = self.config.swagger_api_base_url
            setup_swagger(self._web_app, **ss_kwargs)

        # run the startup tasks
        asyncio.run(self._run_tasks(self._startup_tasks))

        # run the application
        try:
            web.run_app(
                app=self._web_app,
                host=self.config.host,
                port=self.config.port,
                shutdown_timeout=self.config.shutdown_timeout,
                ssl_context=self._ssl_context, loop=self._loop,
                print=logger.debug,
            )
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            # run the shutdown tasks
            asyncio.run(self._run_tasks(self._shutdown_tasks))

    def websocketroute(self, path: str, heartbeat: float = 0.0, autopong: bool = True) -> MokeiWebSocketRoute:
        socket_route = MokeiWebSocketRoute(path, heartbeat=heartbeat, autopong=autopong)

        async def ws(request: Request) -> MokeiWebSocket:
            return await self._handle_websocket(request, socket_route)

        self._routes.get(path)(ws)
        return socket_route

    async def _handle_websocket(self, request: Request, socket_route: MokeiWebSocketRoute) -> MokeiWebSocket:
        """ Called when a new websocket connection is established from a client
        """
        ws = MokeiWebSocket(request, socket_route)

        try:
            await ws.prepare(request)

            socket_route.sockets.add(ws)
            # noinspection PyProtectedMember
            await socket_route._onconnect_handler(ws)
            async for msg in ws:
                msg: WSMessage
                # TODO: handle all message types in the below
                if msg.type is WSMsgType.TEXT:
                    # noinspection PyProtectedMember
                    await socket_route._ontext_handler(ws, msg.data)
                elif msg.type is WSMsgType.BINARY:
                    # noinspection PyProtectedMember
                    await socket_route._onbinary_handler(ws, msg.data)
                elif msg.type is WSMsgType.PING:
                    # noinspection PyProtectedMember
                    await socket_route._onping_handler(ws)
                elif msg.type is WSMsgType.PONG:
                    # noinspection PyProtectedMember
                    await socket_route._onpong_handler(ws)

        except ConnectionResetError:
            return ws

        try:
            socket_route.sockets.remove(ws)
        except KeyError:
            pass  # already removed
        # noinspection PyProtectedMember
        await self._loop.create_task(socket_route._ondisconnect_handler(ws))
        return ws
