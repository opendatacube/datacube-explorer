from ._pages import app
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = (app, __version__)
