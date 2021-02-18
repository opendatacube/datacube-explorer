import datetime
import pathlib
import sys
import uuid
from functools import partial

import rapidjson
import structlog


def init_logging(output_file=None, verbosity: int = 0, cache_logger_on_first_use=True):
    """
    Setup structlog for structured logging output.

    This defaults to stdout as it's the parseable json output of the program.
    Libraries with "unstructured" logs (such as datacube core logging) go to stderr.
    """

    if output_file is None:
        output_file = sys.stdout

    # Note that we can't use functools.partial: it JSONRendering will pass its
    # own 'default' property that overrides our own.
    def lenient_json_dump(obj, *args, **kwargs):
        return rapidjson.dumps(
            obj,
            datetime_mode=rapidjson.DM_ISO8601,
            uuid_mode=rapidjson.UM_CANONICAL,
            number_mode=rapidjson.NM_NATIVE,
            sort_keys=True,
            default=lenient_json_fallback,
        )

    # Direct structlog into standard logging.
    processors = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Coloured output if to terminal, otherwise json
        BetterConsoleRenderer()
        if output_file.isatty()
        else structlog.processors.JSONRenderer(serializer=lenient_json_dump),
    ]

    hide_logging_levels = {
        # Default: show only warnings/critical
        0: ("info", "debug"),
        # One '-v': Show info logging too.
        1: ("debug",),
        # Any more '-v's, show everything.
        2: (),
    }.get(verbosity, ())
    if hide_logging_levels:
        processors.insert(0, partial(_filter_levels, hide_levels=hide_logging_levels))

    structlog.configure(
        processors=processors,
        context_class=dict,
        cache_logger_on_first_use=cache_logger_on_first_use,
        logger_factory=structlog.PrintLoggerFactory(file=output_file),
    )


class BetterConsoleRenderer(structlog.dev.ConsoleRenderer):
    """A console renderer that shows dates in a readable manner."""

    def _repr(self, val):
        if isinstance(val, datetime.datetime):
            return val.isoformat()
        return super()._repr(val)


def _filter_levels(logger, log_method, event_dict, hide_levels=("debug", "info")):
    if log_method in hide_levels:
        raise structlog.DropEvent
    return event_dict


def lenient_json_fallback(obj):
    """Fallback that should always succeed.

    The default fallback will throw exceptions for unsupported types, this one will
    always at least repr() an object rather than throw a NotSerialisableException

    (intended for use in places such as json-based logs where you always want the
    message recorded)
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, (pathlib.Path, uuid.UUID)):
        return str(obj)

    if isinstance(obj, set):
        return list(obj)

    try:
        # Allow class to define their own.
        return obj.to_dict()
    except AttributeError:
        # Same behaviour to structlog default: we always want to log the event
        return repr(obj)
