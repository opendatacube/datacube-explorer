"""Gunicorn config for Prometheus internal metrics"""

import os


def child_exit(server, worker):
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR", False):
        from prometheus_flask_exporter.multiprocess import (
            GunicornInternalPrometheusMetrics,
        )

        GunicornInternalPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)
