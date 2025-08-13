from cognite.powerops.client import PowerOpsClient
from cognite.powerops.client._generated.data_classes import (
    BidConfigurationDayAhead,
    BidConfigurationDayAheadGraphQL,
    DateSpecification,
    GraphQLList,
    MarketConfiguration,
    PartialBidConfiguration,
    PriceAreaDayAhead,
    ShopBasedPartialBidConfigurationList,
)


def graphql_query_bid_configuration(
    external_id: str, space: str = "power_ops_instances", graphql_limit: int = 1000
) -> str:
    query = """
        query MyQuery {{
            getBidConfigurationDayAheadById(
                instance: {{space: "{space}", externalId: "{external_id}"}}
            ) {{
                items {{
                __typename
                    bidDateSpecification {{
                        space
                        externalId
                        createdTime
                        lastUpdatedTime
                        name
                        processingTimezone
                        resultingTimezone
                        floorFrame
                        shiftDefinition
                    }}
                    createdTime
                    space
                    externalId
                    lastUpdatedTime
                    name
                    marketConfiguration {{
                        createdTime
                        externalId
                        lastUpdatedTime
                        maxPrice
                        minPrice
                        name
                        priceSteps
                        priceUnit
                        space
                        tickSize
                        tradeLot
                        timezone
                        timeUnit
                    }}
                    partials(first: {graphql_limit}) {{
                        items {{
                            addSteps
                            space
                            createdTime
                            externalId
                            lastUpdatedTime
                            method
                            name
                            powerAsset {{
                                assetType
                                createdTime
                                displayName
                                externalId
                                lastUpdatedTime
                                name
                                ordering
                                space
                            }}
                        }}
                    }}
                    priceArea {{
                        assetType
                        createdTime
                        displayName
                        externalId
                        lastUpdatedTime
                        name
                        ordering
                        space
                    }}
                }}
            }}
        }}
    """
    return query.format(space=space, external_id=external_id, graphql_limit=graphql_limit)


class TestBidConfiguration:
    def test_bid_configuration_retrieve(self, new_bid_configuration: BidConfigurationDayAhead):
        assert new_bid_configuration is not None
        assert isinstance(new_bid_configuration, BidConfigurationDayAhead)
        assert new_bid_configuration.external_id.startswith("pytest_bid_configuration")
        assert new_bid_configuration.name == "Test Bid Configuration"
        assert new_bid_configuration.market_configuration is None
        assert new_bid_configuration.price_area is None
        assert new_bid_configuration.bid_date_specification is None
        assert new_bid_configuration.partials is None

    def test_bid_configuration_upsert_retrieve_default(
        self,
        power_ops_client: PowerOpsClient,
        new_bid_configuration_shop: BidConfigurationDayAhead,
        new_market_configuration: MarketConfiguration,
        new_price_area: PriceAreaDayAhead,
        new_date_specification: DateSpecification,
        new_partials_shop: ShopBasedPartialBidConfigurationList,
    ):
        retrieved_bid_config = power_ops_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(
            external_id=new_bid_configuration_shop.external_id,
            retrieve_connections="skip",
        )

        assert retrieved_bid_config is not None
        assert isinstance(retrieved_bid_config, BidConfigurationDayAhead)

        assert retrieved_bid_config.market_configuration == new_market_configuration.external_id
        assert retrieved_bid_config.price_area == new_price_area.external_id
        assert retrieved_bid_config.bid_date_specification == new_date_specification.external_id
        # NOTE: retrieve_connections="skip" does not retrieve partials (i.e., it ignores edge connections)
        assert retrieved_bid_config.partials is None

    def test_bid_configuration_upsert_retrieve_identifier(
        self,
        power_ops_client: PowerOpsClient,
        new_bid_configuration_shop: BidConfigurationDayAhead,
        new_market_configuration: MarketConfiguration,
        new_price_area: PriceAreaDayAhead,
        new_date_specification: DateSpecification,
        new_partials_shop: ShopBasedPartialBidConfigurationList,
    ):
        retrieved_bid_config = power_ops_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(
            external_id=new_bid_configuration_shop.external_id,
            retrieve_connections="identifier",
        )

        assert retrieved_bid_config is not None
        assert isinstance(retrieved_bid_config, BidConfigurationDayAhead)

        assert retrieved_bid_config.market_configuration == new_market_configuration.external_id
        assert retrieved_bid_config.price_area == new_price_area.external_id
        assert retrieved_bid_config.bid_date_specification == new_date_specification.external_id
        assert len(retrieved_bid_config.partials) == len(new_partials_shop)

        assert all(partial.external_id in retrieved_bid_config.partials for partial in new_partials_shop)

    def test_bid_configuration_upsert_full(
        self,
        new_bid_configuration_shop: BidConfigurationDayAhead,
        new_market_configuration: MarketConfiguration,
        new_price_area: PriceAreaDayAhead,
        new_date_specification: DateSpecification,
        new_partials_shop: ShopBasedPartialBidConfigurationList,
    ):
        assert new_bid_configuration_shop is not None
        assert isinstance(new_bid_configuration_shop, BidConfigurationDayAhead)

        assert new_bid_configuration_shop.market_configuration == new_market_configuration
        assert new_bid_configuration_shop.price_area == new_price_area
        assert new_bid_configuration_shop.bid_date_specification == new_date_specification
        assert len(new_bid_configuration_shop.partials) == len(new_partials_shop)
        assert all(
            partial.external_id in [p.external_id for p in new_bid_configuration_shop.partials]
            for partial in new_partials_shop
        )

        # NOTE: the partials are returned as type PartialBidConfiguration and NOT ShopBasedPartialBidConfiguration
        assert all(isinstance(partial, PartialBidConfiguration) for partial in new_bid_configuration_shop.partials)

    def test_bid_configuration_graphql(
        self,
        power_ops_client: PowerOpsClient,
        new_bid_configuration_shop: BidConfigurationDayAhead,
    ):
        query = graphql_query_bid_configuration(new_bid_configuration_shop.external_id)
        response = power_ops_client.v1.day_ahead_configuration.graphql_query(query)

        assert response is not None
        assert isinstance(response, GraphQLList)
        assert len(response) == 1

        res_bid_config = response[0]
        assert isinstance(res_bid_config, BidConfigurationDayAheadGraphQL)
        res_bid_config = res_bid_config.as_read()
        assert isinstance(res_bid_config, BidConfigurationDayAhead)

        assert res_bid_config.external_id == new_bid_configuration_shop.external_id
        assert res_bid_config.name == new_bid_configuration_shop.name
        assert isinstance(res_bid_config.market_configuration, MarketConfiguration)
        assert (
            res_bid_config.market_configuration.external_id
            == new_bid_configuration_shop.market_configuration.external_id
        )
        assert isinstance(res_bid_config.price_area, PriceAreaDayAhead)
        assert res_bid_config.price_area.external_id == new_bid_configuration_shop.price_area.external_id
        assert isinstance(res_bid_config.bid_date_specification, DateSpecification)
        assert (
            res_bid_config.bid_date_specification.external_id
            == new_bid_configuration_shop.bid_date_specification.external_id
        )
        assert len(res_bid_config.partials) == len(new_bid_configuration_shop.partials)
        assert all(
            partial.external_id in [p.external_id for p in res_bid_config.partials]
            for partial in new_bid_configuration_shop.partials
        )
