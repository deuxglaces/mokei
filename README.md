# Mokei

Mokei is a very simple Python web framework built on aiohttp.

Aiohttp contains a great async web server, with web framework built in,
but can be complex when compared to other web frameworks.

Mokei makes setting up your routes as simple as using Flask or FastAPI.
It also includes super easy websocket routes with support for text and binary messages,
as well as SocketIO-style event handlers.

## Quick Start

    from mokei import Mokei
    
    app = Mokei()
    
    @app.get('/')
    async def hello():
        return 'Hello, World!'

    if __name__ == '__main__':
        app.run()

## Return JSON data

When the return type from the handler is a `dict`, the response is automatically converted to `application/json`

    @app.get('/status')
    async def status():
        return {
            'status': 'OK',
        }

## Adding Websocket support

Adding a websocket route is as simple as adding a normal get route.

Note that you may send websocket messages to a route or to a single websocket
(compare websocket.send_text and data.send_text below)

    app = Mokei()
    data = app.websocketroute('/data')
    
    @data.onconnect
    async def send_welcome_text(websocket):
        await websocket.send_text('Welcome!')

    @data.ontext
    async def relay_text(websocket, text):
        # log incoming text and relay to all other websockets
        logger.info('Received text %s', text)
        await data.send_text(text, exclude=websocket)

SocketIO-style events are also supported. See `js/mokei.js` for a Javascript handler for Mokei WebSockets.

    app = Mokei()
    data = app.websocketroute('/data')
    
    @data.on('ping')
    async def send_pong(websocket, data):
        update_something(websocket)
        await websocket.send_event('pong', {'timestamp': time.time()})
