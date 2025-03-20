from aiohttp import web

middleware = web.middleware


@middleware
async def allow_cors(request, handler):
    resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
