class CogShopError(Exception):
    """Base exception for all CogShop related errors"""

    pass


class CogWriterError(CogShopError):
    pass


class CogReaderError(CogShopError):
    pass


class CogShopConfigError(CogShopError):
    pass
