class UnsupportedWKTProductCRSError(NotImplementedError):
    """We can't, within Postgis, support arbitrary WKT CRSes at the moment."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
