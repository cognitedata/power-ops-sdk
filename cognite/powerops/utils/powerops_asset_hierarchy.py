from cognite.client.data_classes import Asset


def create_skeleton_asset_hierarchy(
    shop_service_url: str,
    organization_subdomain: str,
    tenant_id: str,
) -> list[Asset]:
    """Creates a skeleton Asset hierarchy for all Assets used by PowerOps"""
    root_external_id = "power_ops"
    configurations_external_id = "configurations"

    if shop_service_url == "https://shop-staging.az-inso-powerops.cognite.ai/submit-run":
        customer = "cognite"
    else:
        customer = organization_subdomain

    configurations_metadata = {
        "shop_service_url": shop_service_url,
        "organization_subdomain": organization_subdomain,
        "customer": customer,
        "tenant_id": tenant_id,
    }
    return [
        Asset(
            external_id=root_external_id,
            name="PowerOps",
        ),
        Asset(
            external_id="price_areas",
            name="Price areas",
            parent_external_id=root_external_id,
        ),
        Asset(
            external_id="watercourses",
            name="Watercourses",
            parent_external_id=root_external_id,
        ),
        Asset(
            external_id="plants",
            name="Plants",
            parent_external_id=root_external_id,
        ),
        Asset(
            external_id="reservoirs",
            name="Reservoirs",
            parent_external_id=root_external_id,
        ),
        Asset(
            external_id="generators",
            name="Generators",
            parent_external_id=root_external_id,
        ),
        Asset(
            external_id=configurations_external_id,
            name="Configurations",
            description="Configurations used for PowerOps",
            parent_external_id=root_external_id,
            metadata=configurations_metadata,
        ),
        Asset(
            external_id="benchmarking_configurations",
            name="Benchmarking configurations",
            parent_external_id=configurations_external_id,
            description="Configurations used in benchmarking processes",
        ),
        Asset(
            external_id="bid_process_configurations",
            name="Bid configurations",
            parent_external_id=configurations_external_id,
            description="Configurations used in bid matrix generation processes",
        ),
        Asset(
            external_id="rkom_bid_process_configurations",
            name="RKOM bid configurations",
            parent_external_id=configurations_external_id,
            description="Configurations used in RKOM bid generation processes",
        ),
        Asset(
            external_id="rkom_bid_combination_configurations",
            name="RKOM bid combination configurations",
            parent_external_id=configurations_external_id,
            description="Configurations for which bids should be combined into a total RKOM bid form",
        ),
        Asset(
            external_id="market_configurations",
            name="Market configurations",
            parent_external_id=configurations_external_id,
            description="Configurations used for different markets",
        ),
    ]
