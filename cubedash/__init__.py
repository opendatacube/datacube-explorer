try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"

from ._model import create_app

__all__ = ("create_app", "__version__")
