Debugging Application Errors
============================

In Production
-------------

**Do not run the development server, or enable the built-in debugger, in
a production environment.** The debugger allows executing arbitrary
Python code from the browser. It's protected by a pin, but that should
not be relied on for security.

Use an error logging tool, such as Sentry, as described in
:ref:`error-logging-tools`, or enable logging and notifications as
described in :doc:`/logging`.

If you have access to the server, you could add some code to start an
external debugger if ``request.remote_addr`` matches your IP. Some IDE
debuggers also have a remote mode so breakpoints on the server can be
interacted with locally. Only enable a debugger temporarily.

.. _sentry-env:

Explorer Sentry setup
----------------------

To enable Sentry reporting set environment

.. py:data:: SENTRY_DSN

    Enable Sentry reporting.

.. py:data:: SENTRY_ENV_TAG

    Add environment for Sentry reporting.

    Default: ``dev-explorer``
