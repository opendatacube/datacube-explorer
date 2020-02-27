from pkg_resources import DistributionNotFound, get_distribution

from ._pages import app

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = "Unknown/Not Installed"

__all__ = (app, __version__)
