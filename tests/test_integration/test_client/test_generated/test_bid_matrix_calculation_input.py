import pytest

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.client._generated.v1.data_classes import (
    BidConfigurationDayAhead,
    GraphQLList,
    MultiScenarioPartialBidMatrixCalculationInput,
    MultiScenarioPartialBidMatrixCalculationInputGraphQL,
    MultiScenarioPartialBidMatrixCalculationInputWrite,
    ShopBasedPartialBidConfiguration,
    ShopBasedPartialBidConfigurationGraphQL,
    ShopBasedPartialBidConfigurationList,
    ShopScenario,
    ShopScenarioGraphQL,
    ShopScenarioSet,
    ShopScenarioSetGraphQL,
    WaterValueBasedPartialBidConfiguration,
    WaterValueBasedPartialBidConfigurationGraphQL,
    WaterValueBasedPartialBidMatrixCalculationInput,
    WaterValueBasedPartialBidMatrixCalculationInputGraphQL,
    WaterValueBasedPartialBidMatrixCalculationInputWrite,
)


def render_water_value_based_partial_bid_matrix_calc_input_query(
    external_id: str, space: str = "power_ops_instances", graphql_limit: int = 1000
) -> str:
    """
    Generate a hard-coded graphQL query for the specified py-generated object.
    """
    query = """
        query MyQuery {{
            getWaterValueBasedPartialBidMatrixCalculationInputById(
                instance: {{space: "{space}", externalId: "{external_id}"}}
            ) {{
            items {{
            __typename
            bidConfiguration {{
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
            bidDate
            space
            createdTime
            externalId
            functionCallId
            functionName
            lastUpdatedTime
            partialBidConfiguration {{
                createdTime
                space
                addSteps
                createdTime
                externalId
                lastUpdatedTime
                method
                name
                powerAsset {{
                    assetType
                    space
                    connectionLosses
                    createdTime
                    displayName
                    externalId
                    feedingFeeTimeSeries {{
                        externalId
                    }}
                    generators(first: {graphql_limit}) {{
                        items {{
                            assetType
                            space
                            createdTime
                            displayName
                            externalId
                            generatorEfficiencyCurve {{
                                externalId
                                space
                                power
                                lastUpdatedTime
                                efficiency
                                createdTime
                            }}
                            lastUpdatedTime
                            name
                            ordering
                            penstockNumber
                            productionMin
                            startStopCost
                            startStopCostTimeSeries {{
                                externalId
                            }}
                            availabilityTimeSeries {{
                                externalId
                            }}
                            turbineEfficiencyCurves (sort: {{head: ASC}} first: {graphql_limit}) {{
                                items {{
                                createdTime
                                space
                                efficiency
                                externalId
                                flow
                                head
                                lastUpdatedTime
                                }}
                            }}
                        }}
                    }}
                    headLossFactor
                    lastUpdatedTime
                    name
                    ordering
                    outletLevel
                    penstockHeadLossFactors
                    productionMax
                    productionMaxTimeSeries {{
                        externalId
                    }}
                    productionMinTimeSeries {{
                        externalId
                    }}
                    productionMin
                    waterValueTimeSeries {{
                        externalId
                    }}
                    outletLevelTimeSeries {{
                        externalId
                    }}
                    inletLevelTimeSeries {{
                        externalId
                    }}
                    headDirectTimeSeries {{
                        externalId
                    }}
                }}
            }}
            workflowExecutionId
            workflowStep
            }}
            }}
        }}
    """
    return query.format(space=space, external_id=external_id, graphql_limit=graphql_limit)


def render_shop_based_partial_bid_matrix_calc_input_query(
    external_id: str, space: str = "power_ops_instances", graphql_limit: int = 1000
) -> str:
    query = """
        query MyQuery {{
            getMultiScenarioPartialBidMatrixCalculationInputById(
                instance: {{space: "{space}", externalId: "{external_id}"}}
            ) {{
                items {{
                __typename
                partialBidConfiguration {{
                    addSteps
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
                space
                scenarioSet {{
                    createdTime
                    endSpecification {{
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
                    externalId
                    lastUpdatedTime
                    name
                    space
                    startSpecification {{
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
                    scenarios(first: {graphql_limit}) {{
                        items {{
                            attributeMappingsOverride(first: {graphql_limit}) {{
                            items {{
                            aggregation
                            attributeName
                            createdTime
                            externalId
                            lastUpdatedTime
                            objectName
                            objectType
                            retrieve
                            space
                            transformations
                            timeSeries {{
                                externalId
                            }}
                            }}
                        }}
                        commands {{
                            commands
                            createdTime
                            externalId
                            lastUpdatedTime
                            name
                            space
                        }}
                        createdTime
                        externalId
                        lastUpdatedTime
                        name
                        source
                        space
                    model {{
                        externalId
                        lastUpdatedTime
                        penaltyLimit
                        name
                        shopVersion
                        space
                        modelVersion
                        model {{
                        externalId
                        }}
                        cogShopFilesConfig(first: {graphql_limit}) {{
                        items {{
                            externalId
                            space
                            createdTime
                            lastUpdatedTime
                            name
                            label
                            fileReference {{
                                externalId
                            }}
                            order
                            isAscii
                        }}
                        }}
                        createdTime
                    }}
                    }}
                }}
                }}
            }}
            bidDate
            createdTime
            externalId
            functionCallId
            functionName
            lastUpdatedTime
            space
            workflowExecutionId
            workflowStep
            bidConfiguration {{
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
                externalId
                lastUpdatedTime
                name
                space
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
                timeUnit
                timezone
                tradeLot
                }}
                priceArea {{
                space
                ordering
                name
                lastUpdatedTime
                externalId
                displayName
                createdTime
                assetType
                mainPriceScenario {{
                    externalId
                }}
                priceScenarios {{
                    externalId
                }}
                }}
            }}
            priceProduction(first: {graphql_limit}) {{
                items {{
                createdTime
                externalId
                lastUpdatedTime
                name
                space
                price {{
                    externalId
                }}
                production {{
                    externalId
                }}
                }}
            }}
            }}
        }}
    }}
    """
    return query.format(space=space, external_id=external_id, graphql_limit=graphql_limit)


@pytest.fixture
def new_multi_scenario_partial(
    power_ops_client: PowerOpsClient,
    new_bid_configuration: BidConfigurationDayAhead,
    new_partials_shop: ShopBasedPartialBidConfigurationList,
) -> MultiScenarioPartialBidMatrixCalculationInput:
    external_id = "pytest_multi_scenario_partial"

    multi_scenario_partial = MultiScenarioPartialBidMatrixCalculationInputWrite(
        external_id=external_id,
        workflow_execution_id="test_workflow_execution",
        workflow_step=10,
        function_call_id="test_function_call",
        function_name="test_function_name",
        bid_date="2023-10-01T00:00:00Z",
        bid_configuration=new_bid_configuration.as_id(),
        partial_bid_configuration=new_partials_shop[0].as_id(),
        price_production=None,
    )
    power_ops_client.v1.upsert(multi_scenario_partial)
    yield (
        power_ops_client.v1.shop_based_day_ahead_bid_process.multi_scenario_partial_bid_matrix_calculation_input.retrieve(
            external_id=external_id
        )
    )
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert (
        power_ops_client.v1.shop_based_day_ahead_bid_process.multi_scenario_partial_bid_matrix_calculation_input.retrieve(
            external_id=external_id
        )
        is None
    )


@pytest.fixture
def new_water_value_based(
    power_ops_client: PowerOpsClient,
    new_bid_configuration: BidConfigurationDayAhead,
    new_partials_water: ShopBasedPartialBidConfigurationList,
) -> MultiScenarioPartialBidMatrixCalculationInput:
    external_id = "pytest_water_value_based"

    multi_scenario_partial = WaterValueBasedPartialBidMatrixCalculationInputWrite(
        external_id=external_id,
        workflow_execution_id="test_workflow_execution",
        workflow_step=10,
        function_call_id="test_function_call",
        function_name="test_function_name",
        bid_date="2023-10-01T00:00:00Z",
        bid_configuration=new_bid_configuration.as_id(),
        partial_bid_configuration=new_partials_water[0].as_id(),
        price_production=None,
    )
    power_ops_client.v1.upsert(multi_scenario_partial)
    yield (
        power_ops_client.v1.water_value_based_day_ahead_bid_process.water_value_based_partial_bid_matrix_calculation_input.retrieve(
            external_id=external_id
        )
    )
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert (
        power_ops_client.v1.water_value_based_day_ahead_bid_process.water_value_based_partial_bid_matrix_calculation_input.retrieve(
            external_id=external_id
        )
        is None
    )


class TestBidMatrixCalculationInput:
    def test_shop_graphql(
        self,
        power_ops_client: PowerOpsClient,
        new_multi_scenario_partial: MultiScenarioPartialBidMatrixCalculationInput,
    ):
        query = render_shop_based_partial_bid_matrix_calc_input_query(new_multi_scenario_partial.external_id)
        response = power_ops_client.v1.shop_based_day_ahead_bid_process.graphql_query(query)

        assert response is not None
        assert isinstance(response, GraphQLList)
        assert len(response) == 1

        response_obj = response[0]
        assert isinstance(response_obj, MultiScenarioPartialBidMatrixCalculationInputGraphQL)
        assert isinstance(response_obj.partial_bid_configuration, ShopBasedPartialBidConfigurationGraphQL)
        assert isinstance(response_obj.partial_bid_configuration.scenario_set, ShopScenarioSetGraphQL)
        scenarios = response_obj.partial_bid_configuration.scenario_set.scenarios
        assert all(isinstance(scenario, ShopScenarioGraphQL) for scenario in scenarios)

        response_obj_read = response_obj.as_read()
        assert isinstance(response_obj_read, MultiScenarioPartialBidMatrixCalculationInput)
        assert isinstance(response_obj_read.partial_bid_configuration, ShopBasedPartialBidConfiguration)
        assert isinstance(response_obj_read.partial_bid_configuration.scenario_set, ShopScenarioSet)
        scenarios = response_obj_read.partial_bid_configuration.scenario_set.scenarios
        assert all(isinstance(scenario, ShopScenario) for scenario in scenarios)

    def test_water_graphql(
        self,
        power_ops_client: PowerOpsClient,
        new_water_value_based: WaterValueBasedPartialBidMatrixCalculationInput,
    ):
        query = render_water_value_based_partial_bid_matrix_calc_input_query(new_water_value_based.external_id)
        response = power_ops_client.v1.water_value_based_day_ahead_bid_process.graphql_query(query)

        assert response is not None
        assert isinstance(response, GraphQLList)
        assert len(response) == 1

        response_obj = response[0]
        assert isinstance(response_obj, WaterValueBasedPartialBidMatrixCalculationInputGraphQL)
        assert isinstance(response_obj.partial_bid_configuration, WaterValueBasedPartialBidConfigurationGraphQL)

        response_obj_read = response_obj.as_read()
        assert isinstance(response_obj_read, WaterValueBasedPartialBidMatrixCalculationInput)
        assert isinstance(response_obj_read.partial_bid_configuration, WaterValueBasedPartialBidConfiguration)
