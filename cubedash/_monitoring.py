import functools
import inspect
import sys
import time

import flask
from sqlalchemy import event

from . import _model
from ._utils import alchemy_engine

_INITIALISED = False


# Add server timings to http headers.
def init_app_monitoring():
    # This affects global flask app settings.
    # pylint: disable=global-statement
    global _INITIALISED

    if _INITIALISED:
        return

    _INITIALISED = True

    @_model.app.before_request
    def time_start():
        flask.g.start_render = time.time()
        flask.g.datacube_query_time = 0
        flask.g.datacube_query_count = 0

    @event.listens_for(alchemy_engine(_model.STORE.index), "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        conn.info.setdefault("query_start_time", []).append(time.time())

    @event.listens_for(alchemy_engine(_model.STORE.index), "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if flask.has_app_context():
            flask.g.datacube_query_time += time.time() - conn.info[
                "query_start_time"
            ].pop(-1)
            flask.g.datacube_query_count += 1
        # print(f"===== {flask.g.datacube_query_time*1000} ===: {repr(statement)}")

    @_model.app.after_request
    def time_end(response: flask.Response):
        render_time = time.time() - flask.g.start_render
        response.headers.add_header(
            "Server-Timing",
            f"app;dur={render_time*1000},"
            f'odcquery;dur={flask.g.datacube_query_time*1000};desc="ODC query time",'
            f"odcquerycount_{flask.g.datacube_query_count};"
            f'desc="{flask.g.datacube_query_count} ODC queries"',
        )
        return response

    def decorate_all_methods(cls, decorator):
        """
        Decorate all public methods of the class with the given decorator.
        """
        for name, clasification, _clz, attr in inspect.classify_class_attrs(cls):
            if clasification == "method" and not name.startswith("_"):
                setattr(cls, name, decorator(attr))
        return cls

    def print_datacube_query_times():
        from click import style

        def with_timings(function):
            """
            Decorate the given function with a stderr print of timing
            """

            @functools.wraps(function)
            def decorator(*args, **kwargs):
                start_time = time.time()
                ret = function(*args, **kwargs)
                duration_secs = time.time() - start_time
                print(
                    f"== Index Call == {style(function.__name__, bold=True)}: "
                    f"{duration_secs*1000}",
                    file=sys.stderr,
                    flush=True,
                )
                return ret

            return decorator

        # Print call time for all db layer calls.
        import datacube.drivers.postgres._api as api

        decorate_all_methods(api.PostgresDbAPI, with_timings)

    print_datacube_query_times()
