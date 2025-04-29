import collections.abc
import dataclasses
import inspect
import json
import logging
import typing

import pythonjsonlogger.defaults as d
import pythonjsonlogger.json


class Once:
    _has_run: bool = False
    _fn: collections.abc.Callable[[], None]

    def __init__(self, fn: collections.abc.Callable[[], None]) -> None:
        self._fn = fn

    def run(self) -> None:
        if not self._has_run:
            self._fn()
            self._has_run = True


def setup_logging() -> None:
    logger = logging.getLogger()

    log_handler = logging.StreamHandler()
    formatter = pythonjsonlogger.json.JsonFormatter(
        "{message}{asctime}{exc_info}{levelname}{funcname}{lineno}{module}"
        "{name}{process}{processName}{stack_info}{thread}{threadName}"
        "{taskName}",
        style="{",
        json_default=json_default,
    )
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    logger.setLevel("INFO")


def log_extra(**kwargs: typing.Any) -> dict[str, typing.Any]:
    return {"extra": kwargs}


@typing.runtime_checkable
class CustomJsonSerializable(typing.Protocol):
    def obj_json_default(self) -> typing.Any: ...


def json_default(obj: typing.Any) -> typing.Any:
    """
    Function to use for json serializer json_default.  Given an object it
    returns a new object to serialize in its place.

    We will check to see if the object has a callable attribute obj_json_default
    and if so call that.

    Raising an exception here results in the object being serialized as-is
    """

    jd = getattr(obj, "obj_json_default", None)
    if jd is None:
        return _python_json_logger_default(obj)

    # let it raise if we can't get a signature.  An exception causes it to
    # serialize the object as-is
    sig: inspect.Signature = inspect.signature(jd)

    if sig.parameters:
        # expect 0 arguments.  "self" is removed by inspect.signature
        return _python_json_logger_default(obj)

    return obj.obj_json_default()


_encoder = json.JSONEncoder()


def _use_obj_json_default(obj: typing.Any) -> bool:
    jd = getattr(obj, "obj_json_default", None)
    if jd is None:
        return False

    # let it raise if we can't get a signature.  An exception causes it to
    # serialize the object as-is
    sig: inspect.Signature = inspect.signature(jd)

    if sig.parameters:
        # expect 0 arguments.  "self" is removed by inspect.signature
        return False

    return True


class DataclassInstance(typing.Protocol):
    """Interned from standard typeshed"""

    __dataclass_fields__: typing.ClassVar[dict[str, dataclasses.Field[typing.Any]]]


def _use_dataclass_default(obj: typing.Any) -> typing.TypeIs[DataclassInstance]:
    """Default check function for dataclass instances"""
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


def _dataclass_default(obj: DataclassInstance) -> dict[str, typing.Any]:
    """Default function for dataclass instances

    Args:
        obj: object to handle

    Interned from python-json-logger and modified
    """
    return {
        field.name: _python_json_logger_default(getattr(obj, field.name))
        for field in dataclasses.fields(obj)
    }


def _python_json_logger_default(o: typing.Any) -> typing.Any:
    """Interned from python-json-logger and modified"""
    if _use_obj_json_default(o):
        return _python_json_logger_default(o.obj_json_default())

    if d.use_datetime_any(o):
        return d.datetime_any(o)

    if d.use_exception_default(o):
        return d.exception_default(o)

    if d.use_traceback_default(o):
        return d.traceback_default(o)

    if d.use_enum_default(o):
        return d.enum_default(o)

    if d.use_bytes_default(o):
        return d.bytes_default(o)

    if _use_dataclass_default(o):
        return _dataclass_default(o)

    if d.use_type_default(o):
        return d.type_default(o)

    try:
        return _encoder.default(o)
    except TypeError:
        return d.unknown_default(o)
