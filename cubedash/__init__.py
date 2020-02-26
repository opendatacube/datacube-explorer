from ._pages import app
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = 'Unknown/Not Installed'

__all__ = (app, __version__)
