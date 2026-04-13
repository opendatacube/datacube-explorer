try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"

__all__ = ["__version__"]
