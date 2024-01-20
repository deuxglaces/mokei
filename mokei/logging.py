import sys as _sys
import time as _time
import logging as _logging
from logging import *

_logging.Formatter.converter = _time.gmtime
_logging.basicConfig(
    stream=_sys.stdout,
    level=INFO,
    format='%(asctime)s %(levelname)s - %(message)s'
)

_aiohttp_loggers = (
    'aiohttp.access',
    'aiohttp',
    'aiohttp.client',
    'aiohttp.internal',
    'aiohttp.server',
    'aiohttp.web',
    'aiohttp.websocket',
)

for logger in _aiohttp_loggers:
    getLogger(logger).setLevel(ERROR)
