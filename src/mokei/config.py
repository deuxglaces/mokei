import dataclasses
import pathlib
from typing import Callable, Optional


@dataclasses.dataclass
class MokeiConfig:
    host: str = '0.0.0.0'
    port: int = 8000
    certfile: Optional[pathlib.Path] = None
    keyfile: Optional[pathlib.Path] = None
    password: Optional[Callable[[], str | bytes | bytearray] | str | bytes | bytearray] = None
    use_swagger: bool = True
    swagger_api_base_url: str = None
    use_templates: bool = True
    middlewares: list = dataclasses.field(default_factory=list)
    template_dir: Optional[str | pathlib.Path] = None
    static_dirs: dict[str, str | pathlib.Path] = dataclasses.field(default_factory=dict)
    shutdown_timeout: float = 5.0
