from typing import Optional

import aiohttp
from aiohttp.client import ClientSession, ClientResponse
from yarl import URL

from .datatypes import JsonDict


class MokeiClient:
    def __init__(self, verify_ssl: bool = True,
                 headers: Optional[dict[str, str]] = None,
                 cookies: Optional[dict[str, str]] = None):
        self._session: ClientSession = ClientSession()
        self.verify_ssl = verify_ssl
        self.headers = headers or {}
        self.cookies = cookies or {}

    async def start_new_session(self, keep_headers: bool = False, keep_cookies: bool = False) -> None:
        if self._session:
            await self._session.close()
        if not keep_headers:
            self.headers.clear()
        if not keep_cookies:
            self.cookies.clear()

    async def post_form_data(self, url: str | URL, data: dict) -> ClientResponse:
        form_data = aiohttp.FormData()
        for key, value in data.items():
            form_data.add_field(key, value)
        async with self._session.post(url, data, headers=self.headers, verify_ssl=self.verify_ssl) as response:
            return response

    async def post_json(self, url: str | URL, jsondict: JsonDict) -> ClientResponse:
        async with self._session.post(url, json=jsondict, headers=self.headers, verify_ssl=self.verify_ssl) as response:
            return response

    async def get(self, url: str | URL) -> ClientResponse:
        async with self._session.get(url, headers=self.headers, verify_ssl=self.verify_ssl) as response:
            return response

    def set_header(self, header_name: str, header_value: str) -> None:
        self.headers[header_name] = header_value

    def set_cookie(self, cookie_name: str, cookie_value: str) -> None:
        self.cookies[cookie_name] = cookie_value

    def set_headers(self, headers: dict[str, str]) -> None:
        for header, value in headers.items():
            self.set_header(header, value)

    def set_cookies(self, cookies: dict[str, str]) -> None:
        for cookie, value in cookies.items():
            self.set_cookie(cookie, value)
