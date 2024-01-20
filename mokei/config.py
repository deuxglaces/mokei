import dataclasses
import pathlib
from typing import Callable, Optional

from .middlewares import mokei_resp_type_middleware


def _get_default_middlewares() -> list:
    return [
        mokei_resp_type_middleware,
    ]


@dataclasses.dataclass
class Config:
    host: str = '0.0.0.0'
    port: int = 8000
    certfile: Optional[pathlib.Path] = None
    keyfile: Optional[pathlib.Path] = None
    password: Optional[Callable[[], str | bytes | bytearray] | str | bytes | bytearray] = None
    middlewares: list = dataclasses.field(default_factory=_get_default_middlewares)
    use_swagger: bool = True
    use_templates: bool = True
    template_dir: Optional[str | pathlib.Path] = None
    static_dirs: dict[str, str | pathlib.Path] = dataclasses.field(default_factory=dict)
