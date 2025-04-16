import functools
import inspect
import sys
import time

import flask
from sqlalchemy import event

from . import _model

_INITIALISED = False


# Add server timings to http headers.
def init_app_monitoring(app: flask.Flask) -> None:
    # This affects global flask app settings.
    # pylint: disable=global-statement
    global _INITIALISED

    @app.before_request
    def time_start() -> None:
        flask.g.start_render = time.time()
        flask.g.datacube_query_time = 0
        flask.g.datacube_query_count = 0

    @app.after_request
    def time_end(response: flask.Response):
        render_time = time.time() - flask.g.start_render
        response.headers.add_header(
            "Server-Timing",
            f"app;dur={render_time * 1000},"
            f'odcquery;dur={flask.g.datacube_query_time * 1000};desc="ODC query time",'
            f"odcquerycount_{flask.g.datacube_query_count};"
            f'desc="{flask.g.datacube_query_count} ODC queries"',
        )
        return response

    if _INITIALISED:
        return

    _INITIALISED = True

    @event.listens_for(_model.STORE.e_index.engine, "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ) -> None:
        conn.info.setdefault("query_start_time", []).append(time.time())

    @event.listens_for(_model.STORE.e_index.engine, "after_cursor_execute")
    def after_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ) -> None:
        if flask.has_app_context() and hasattr(flask.g, "datacube_query_time"):
            flask.g.datacube_query_time += time.time() - conn.info[
                "query_start_time"
            ].pop(-1)
            flask.g.datacube_query_count += 1
        # print(f"===== {flask.g.datacube_query_time*1000} ===: {repr(statement)}")

    def decorate_all_methods(cls, decorator):
        """
        Decorate all public methods of the class with the given decorator.
        """
        for name, clasification, _clz, attr in inspect.classify_class_attrs(cls):
            if clasification == "method" and not name.startswith("_"):
                setattr(cls, name, decorator(attr))
        return cls

    def print_datacube_query_times() -> None:
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
                    f"{duration_secs * 1000}",
                    file=sys.stderr,
                    flush=True,
                )
                return ret

            return decorator

        # Print call time for all db layer calls.
        decorate_all_methods(_model.STORE.e_index.db_api, with_timings)

    print_datacube_query_times()
