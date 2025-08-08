"""
This script "manually" updates bid matrix information and partial bid matrix information with properties.
You need to specify what properties you want to link to the matrix information objects.
"""

from cognite.client.data_classes import TimeSeries, TimeSeriesList
from cognite.client import CogniteClient
from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.data_classes import BidMatrixInformationWrite


def initialize_clients():
    """
    Initializing the PowerOps and CDF clients.

    Returns:
        tuple: A tuple containing the PowerOps client and the CDF client.
    """
    power_client = PowerOpsClient.from_settings()
    cdf_client = power_client.cdf
    return power_client, cdf_client


def update_partial_bid_matrix_information(
    power_client: PowerOpsClient, ex_id: str, plant_ex_id: str, bidconfig_ex_id: str
):
    """
    Updating partial bid matrix information with power asset and partial bid configuration.

    Args:
        power_client (PowerOpsClient): An authenticated PowerOps client.
        ex_id (str): External ID of the partial bid matrix information object.
        plant_ex_id (str): External ID of the power asset.
        bidconfig_ex_id (str): External ID of the partial bid configuration.
    """
    the_partial_bidmatrix = power_client.v1.day_ahead_bid.partial_bid_matrix_information.retrieve(external_id=ex_id)
    the_partial_bidmatrix.power_asset = plant_ex_id
    the_partial_bidmatrix.partial_bid_configuration = bidconfig_ex_id

    print(f"Power Asset: {the_partial_bidmatrix.power_asset}")
    print(f"Partial Bid Configuration: {the_partial_bidmatrix.partial_bid_configuration}")

    the_partial_bidmatrix_write = the_partial_bidmatrix.as_write()

    # Upsert the partial bid matrix information object:
    power_client.v1.upsert(the_partial_bidmatrix_write, replace=False)


def update_bid_matrix_information(power_client: PowerOpsClient, ex_id: str):
    """
    Update bid matrix information.

    Args:
        power_client (PowerOpsClient): An authenticated PowerOps client.
        ex_id (str): External ID of the bid matrix information object.
    """
    the_bidmatrixinfo = power_client.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id=ex_id)
    the_bidmatrixinfo_write = the_bidmatrixinfo.as_write()
    print(f"Bid Matrix: {the_bidmatrixinfo_write}")

    # Upset the bid matrix information object:
    power_client.v1.upsert(the_bidmatrixinfo_write, replace=False)


def main():
    """
    Main function to execute the script.

    This function initializes the clients, updates partial bid matrix information,
    and updates bid matrix information.
    """
    ex_id = "POWEROPS_finalised-partial-bid-matrix_Lund_2023-5-8_536e"
    plant_ex_id = "plant_information_lund"
    bidconfig_ex_id = "shop_based_partial_bid_configuration_lund_2"

    power_client, cdf_client = initialize_clients()

    update_partial_bid_matrix_information(power_client, ex_id, plant_ex_id, bidconfig_ex_id)
    update_bid_matrix_information(power_client, ex_id)


if __name__ == "__main__":
    main()


# power_client = PowerOpsClient.from_settings()
# cdf_client = power_client.cdf

# ex_id = "POWEROPS_finalised-partial-bid-matrix_Lund_2023-5-8_536e"
# plant_ex_id = "plant_information_lund"
# bidconfig_ex_id = "shop_based_partial_bid_configuration_lund_2"


# lund_partial_bidmatrix = power_client.v1.day_ahead_bid.partial_bid_matrix_information.retrieve(external_id=ex_id)

# lund_partial_bidmatrix.power_asset = plant_ex_id
# lund_partial_bidmatrix.partial_bid_configuration = bidconfig_ex_id

# print(lund_partial_bidmatrix.power_asset)
# print(lund_partial_bidmatrix.partial_bid_configuration)


# lund_partial_bidmatrix = lund_partial_bidmatrix.as_write()
# power_client.v1.upsert(lund_partial_bidmatrix, replace=False)


# lund_bidmatrixinfo = power_client.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id=ex_id)
# lund_bidmatrixinfo = lund_bidmatrixinfo.as_write()
# power_client.v1.upsert(lund_bidmatrixinfo, replace=False)
