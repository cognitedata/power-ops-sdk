"""
This package contains the v2 version of the models in the resync package. All of these models
are based on Data Modeling in CDF.

* Benchmarking - This is the data model version of the Benchmarking part of the MarketModel in v1.
* CogSHOP - This is the updated version of the CogShop1Asset in v1.
"""


from .production_dm import PowerAssetModelDM

__all__ = ["PowerAssetModelDM"]
