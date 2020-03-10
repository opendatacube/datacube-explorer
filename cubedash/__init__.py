try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Not-Installed"

from ._pages import app

__all__ = (app, __version__)
