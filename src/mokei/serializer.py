import datetime
import dataclasses
import collections.abc
import decimal
import enum
import json
import pathlib
from typing import Any, Type, TypeVar, get_args, get_origin, Generic

_T = TypeVar('_T')


def dec_to_str(dec: decimal.Decimal, min_dp: int = 0) -> str:
    """Custom function to convert decimal.Decimal to str in a consistant format with a
    min number of decimal places"""
    if dec == decimal.Decimal('0'):
        # special case to deal with zero
        sign = 0
        digits = ['0'] + ['0'] * min_dp
        exponent = - min_dp
    else:
        sign, digits_tup, exponent = dec.as_tuple()
        digits = [str(digit) for digit in digits_tup]
        while digits[-1] == '0' and len(digits) > 1:
            digits.pop()
            exponent += 1
        add_zero_after = max(min_dp + exponent, 0)
        exponent -= add_zero_after
        add_zero_before = abs(min(exponent + len(digits) + add_zero_after - 1, 0))
        digits = ['0'] * add_zero_before + digits + ['0'] * add_zero_after
    if exponent < 0:
        return ('-' if sign else '') + ''.join(digits[:exponent]) + '.' + ''.join(digits[exponent:])
    return ('-' if sign else '') + ''.join(digits) + '0' * exponent


class Serializer:
    def __init__(self):
        self._DATA_CAST: dict[type, tuple[collections.abc.Callable, collections.abc.Callable]] = {
            datetime.date: (datetime.date.isoformat, datetime.date.fromisoformat),
            datetime.time: (datetime.time.isoformat, datetime.time.fromisoformat),
            datetime.datetime: (datetime.datetime.isoformat, datetime.datetime.fromisoformat),
            decimal.Decimal: (dec_to_str, decimal.Decimal),
        }

    def register_type(self, type_, store_fn: collections.abc.Callable, retrive_fn: collections.abc.Callable):
        self._DATA_CAST[type_] = (store_fn, retrive_fn)

    def to_str(self, obj: Any) -> str:
        return json.dumps(self._ser(obj))

    def to_dict(self, obj: Any) -> dict:
        return self._ser(obj)

    def to_file(self, obj: Any, filepath: str | pathlib.Path) -> None:
        file_data = self.to_str(obj)
        with open(filepath, mode='w') as output_file:
            output_file.write(file_data)

    def from_str(self, cls: Type[_T], json_str: str) -> _T:
        return self._deser(cls, json.loads(json_str))

    def from_dict(self, cls: Type[_T], json_dict: dict) -> _T:
        return self._deser(cls, json_dict)

    def from_file(self, cls: Type[_T], filepath: str | pathlib.Path) -> _T:
        with open(filepath) as input_file:
            return self.from_str(cls, input_file.read())

    # noinspection PyUnresolvedReferences,PyTypeChecker
    def _ser(self, v):
        if v is None:
            return None
        if dataclasses.is_dataclass(v.__class__):
            return self._ser(v.__dict__)
        if isinstance(v, enum.Enum):
            return v.value
        if v.__class__ in self._DATA_CAST:
            return self._DATA_CAST[v.__class__][0](v)
        if any(isinstance(v, c) for c in (list, tuple, set)):
            return [self._ser(item) for item in v]
        if isinstance(v, dict):
            return {self._ser(k): self._ser(va) for k, va in v.items()}
        return v

    def _deser(self, c, v):
        if dataclasses.is_dataclass(c):
            return None if v is None else c(**{
                k: self._deser(c.__annotations__[k], v.get(k)) for k in v
            })
        if c in self._DATA_CAST:
            return self._DATA_CAST[c][1](v)
        if origin := get_origin(c):
            if issubclass(c.__origin__, Generic):
                if args := get_args(c):
                    return self._deser(args[0], v)
            if origin is list:
                # type hint must be of form list[str] etc. with single type of value
                return [self._deser(c.__args__[0], item) for item in v]
            if origin is set:
                # type hint must be of form set[str] etc. with single type of value
                return {self._deser(c.__args__[0], item) for item in v}
            if origin is tuple:
                # type hint must be of form tuple[str, datetime.date, int]
                # etc. with type for each of the FIXED no. of items
                return tuple(self._deser(item_cls, item) for item_cls, item in zip(c.__args__, v))
            if origin is collections.defaultdict:
                # type hint must be of form collections.defaultdict[str, int] etc. with types of key and value mentioned
                # only works for defaultdict whose factory is the second type arg
                return collections.defaultdict(c.__args__[1], {
                    self._deser(c.__args__[0], k): self._deser(c.__args__[1], v) for k, v in
                    v.items()
                })
            if origin is dict:
                # type hint must be of form dict[str, int] etc. with types of key and value mentioned
                return {
                    self._deser(c.__args__[0], k): self._deser(c.__args__[1], v) for k, v in
                    v.items()
                }
        if issubclass(c, enum.Enum):
            return c(v)
        return v
