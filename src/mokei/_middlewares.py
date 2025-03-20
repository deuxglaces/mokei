import dataclasses
import pathlib

import aiohttp.web_response
import aiohttp.web
import pydantic

from .serializer import Serializer
from .request import Request

middleware = aiohttp.web.middleware
serializer = Serializer()


# noinspection PyProtectedMember
@middleware
async def convert_to_mokei_request(request, handler):
    mokei_request = Request(
        request._message,
        request._payload,
        request._protocol,
        request._payload_writer,
        request._task,
        request._loop,
        client_max_size=request._client_max_size,
        state=request._state.copy(),
    )
    for key, item in request.__dict__.items():
        setattr(mokei_request, key, item)
    return await handler(mokei_request)


@middleware
async def mokei_resp_type_middleware(request, handler):
    resp = await handler(request)
    if isinstance(resp, tuple) and len(resp) == 2 and isinstance(resp[1], int):
        status = resp[1]
        resp = resp[0]
    else:
        status = 200
    if dataclasses.is_dataclass(resp):
        resp = serializer.to_dict(resp)
    if isinstance(resp, pathlib.Path):
        if not resp.exists():
            raise aiohttp.web.HTTPNotFound()
        with open(resp, mode='rb') as file:
            file_resp = aiohttp.web.Response(
                body=file.read(),
                headers={
                    'Content-Disposition': f'attachment; filename="{resp.name}"',
                    'Content-Type': 'application/octet-stream',
                }
            )
        resp = file_resp
    elif isinstance(resp, pydantic.BaseModel):
        resp = aiohttp.web.Response(
            body=resp.model_dump_json(),
            headers={
                'Content-Type': 'application/json',
            }
        )
    elif isinstance(resp, dict):
        resp = aiohttp.web.json_response(resp, status=status)
    elif isinstance(resp, str):
        resp = aiohttp.web.Response(body=resp, status=status)
    resp.headers.setdefault('Server', 'Server')
    return resp
