from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from cognite.client import CogniteClient
from cognite.client.data_classes.functions import FunctionCall
from cognite.client.utils import ms_to_datetime
from utils import filters_to_str, nested_get

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.client._generated.data_classes import (
    ShopPreprocessorInputWrite,
    ShopTriggerInputWrite,
)

st.set_page_config(page_title="View Shop config (scenario)", layout="wide")
client = CogniteClient()
power_ops_client = PowerOpsClient(client=client)

if "exp_scenario_selector_expanded" not in st.session_state:
    st.session_state["exp_scenario_selector_expanded"] = True
if "exp_scenario_explorer_expanded" not in st.session_state:
    st.session_state["exp_scenario_explorer_expanded"] = True
scenario_external_id = ""

exp_scenario_selector = st.expander(
    expanded=st.session_state["exp_scenario_selector_expanded"],
    label="Find ShopScenario instance",
)
exp_scenario_explorer = st.expander(
    expanded=st.session_state["exp_scenario_explorer_expanded"],
    label="Look at ShopScenario",
)


def run_shop(
    power_ops_client: PowerOpsClient,
    scenario_external_id: str,
    start_time: str,
    end_time: str,
) -> FunctionCall:
    _now = datetime.now()
    name = f"{scenario_external_id.replace('shop_scenario:', '')}_{start_time[0:10]}_{_now.isoformat()}"
    shop_trigger_input = ShopTriggerInputWrite(
        external_id=f"shop_trigger_input:{name}",
        preprocessorInput=ShopPreprocessorInputWrite(
            external_id=f"shop_preprocessor_input:{name}",
            functionName="preprocessor",
            scenario=scenario_external_id,
            startTime=start_time,
            endTime=end_time,
            workflowExecutionId="manual_streamlit",
            workflow_step=-1,
            functionCallId="_",
        ),
        functionName="shop_trigger",
        workflowExecutionId="manual_streamlit",
        workflowStep=-1,
        functionCallId="_",
    )
    power_ops_client.v1.upsert(shop_trigger_input)
    return power_ops_client.cdf.functions.call(
        external_id="shop_trigger",
        data={"shop_trigger_input_instance_external_id": shop_trigger_input.external_id},
    )


_now = datetime.now()
suggested_start_time = _now.strftime("%Y-%m-%d 23:00:00")
suggested_end_time = (_now + timedelta(days=7)).strftime("%Y-%m-%d 23:00:00")

with exp_scenario_selector:
    col_filters, col_results = st.columns([2, 10])
    with col_filters:
        #################################################################################### Time series search options
        last_updated_after_input = st.text_input("Last updated after")
        if last_updated_after_input:
            try:
                last_updated_after = datetime.fromisoformat(last_updated_after_input)
            except:
                last_updated_after = None
                st.warning("Date must be in iso format (YYYY-MM-DDThh:mm:ss[+zh:zm])")
        else:
            last_updated_after = None

        model_name_prefix_input = st.text_input(label="Model name prefix")
        scenario_name_prefix_input = st.text_input(label="Scenario name prefix")
        scenario_limit: int = st.number_input(label="Number of results", value=20, max_value=1000, min_value=1, step=1)

    with col_results:
        filters = []
        if last_updated_after:
            filters.append('{lastUpdatedTime: {gte: "' + last_updated_after.isoformat() + '"}}')
        if scenario_name_prefix_input:
            filters.append(
                '{name: {prefix: "' + scenario_name_prefix_input + '"}}',
            )
        if model_name_prefix_input:
            filters.append(
                '{model: {name: {prefix: "' + model_name_prefix_input + '"}}}',
            )

        filters_str = filters_to_str(filters)

        query = (
            """query ShopScenario {
            listShopScenario("""
            + filters_str
            + """ first: """
            + str(scenario_limit)
            + """) {
                items{
                    externalId
                    name
                    model{
                        externalId
                        name
                    }
                }
            }
        }"""
        )
        shop_scenarios = client.data_modeling.graphql.query(("power_ops_core", "all_PowerOps", "1"), query)[
            "listShopScenario"
        ]["items"]

        file_external_id = "not selected"
        col_widths = (2, 2, 1, 1)
        colms = st.columns(col_widths)
        fields = ["Scenario", "Model", "Select", "Run"]
        for col, field_name in zip(colms, fields, strict=False):
            # header
            col.write(field_name)

        for ii in range(len(shop_scenarios)):
            shop_scenario = shop_scenarios[ii]
            col_scen_name, col_model_name, col_select, col_run = st.columns(col_widths)
            col_scen_name.write(shop_scenario.get("name"))
            col_scen_name.caption(shop_scenario["externalId"])
            col_model_name.write(nested_get(shop_scenario, ["model", "name"]))
            col_model_name.caption(nested_get(shop_scenario, ["model", "externalId"]))
            with col_select:
                select_scenario = st.button("Select", key=f"select{ii}")
                if select_scenario:
                    scenario_external_id = shop_scenario["externalId"]
                    st.session_state["exp_scenario_selector_expanded"] = False
                    st.session_state["exp_scenario_explorer_expanded"] = True
            with col_run:
                with st.popover(label="Run..."):
                    scenario_ext_id_input = st.text_input(
                        "Scenario external ID",
                        value=shop_scenario["externalId"],
                        key=f"scen{ii}",
                    )
                    start_time = st.text_input(
                        "Start time (YYYY-MM-DD hh:mm:ss) UTC",
                        key=f"start{ii}",
                        value=suggested_start_time,
                    )
                    end_time = st.text_input(
                        "End time (YYYY-MM-DD hh:mm:ss) UTC",
                        key=f"end{ii}",
                        value=suggested_end_time,
                    )
                    if st.button("Run", key=f"run{ii}"):
                        fc = run_shop(
                            power_ops_client=power_ops_client,
                            scenario_external_id=scenario_ext_id_input,
                            start_time=start_time,
                            end_time=end_time,
                        )
                        function_call_response = fc.get_response()
                        st.write(function_call_response)
                        function_call_log = fc.get_logs()
                        st.write(function_call_log)


def load_scenario_instance(scenario_external_id: str):
    query = (
        """query ShopScenario{
      listShopScenario(filter: {externalId: {eq: """
        + '"'
        + scenario_external_id
        + '"'
        + """}}){
        items{
          externalId
          name
          attributeMappingsOverride{
            items{
              objectType
              objectName
              attributeName
              timeSeries{
                externalId
              }
              retrieve
              aggregation
              transformations
            }
          }
          model{
            externalId
            name
            model{
              externalId
            }
            baseAttributeMappings{
              items{
                objectType
                objectName
                attributeName
                timeSeries{
                  externalId
                }
                retrieve
                aggregation
                transformations
              }
            }
          }
          commands{
            externalId
            name
            commands
          }
        }
      }
    }"""
    )
    scenarios = client.data_modeling.graphql.query(("power_ops_core", "all_PowerOps", "1"), query)["listShopScenario"][
        "items"
    ]
    if len(scenarios) > 1:
        st.write("Found more than one scenario with that external ID.")

    st.session_state["scenario_dict"] = scenarios[0]
    st.session_state["loaded_scenario"] = scenario_external_id


with exp_scenario_explorer:
    scenario_external_id = st.text_input(label="Scenario external ID:", value=scenario_external_id)
    if scenario_external_id or "loaded_scenario" in st.session_state:
        load_scenario = st.button(
            "Load scenario",
            on_click=lambda: load_scenario_instance(scenario_external_id),
        )
        if "scenario_dict" in st.session_state:
            if scenario_external_id != st.session_state.get("loaded_scenario"):
                st.write(
                    f"Still showing contents from scenario {st.session_state['loaded_scenario']} - not the selected one"
                )
            scenario_dict = st.session_state.scenario_dict

            mapping_overrides = scenario_dict["attributeMappingsOverride"]["items"]
            base_mappings = nested_get(scenario_dict, ["model", "baseAttributeMappings", "items"])
            for mapping in mapping_overrides:
                mapping["source"] = "scenario"
            for mapping in base_mappings:
                mapping["source"] = "model"
            mappings_df = pd.DataFrame(mapping_overrides + base_mappings).sort_values(
                ["objectType", "objectName", "attributeName", "source"]
            )
            mappings_dict = mappings_df.to_dict(orient="records")

            latest_ts_values = client.time_series.data.retrieve_latest(
                external_id=list(
                    {nested_get(mapping, ["timeSeries", "externalId"]) for mapping in mappings_dict} - {None}
                )
            )
            latest_ts_values_dict = {t.external_id: (t.timestamp[0], t.value[0]) for t in latest_ts_values}

            col_widths = (2, 2, 2, 4, 3, 1, 1, 4, 1)
            colms = st.columns(col_widths)
            fields = [
                "Object type",
                "Object name",
                "Attribute",
                "Time series",
                "Latest datapoint",
                "Retrieve",
                "Aggr.",
                "Transformations",
                "Source",
            ]
            for col, field_name in zip(colms, fields, strict=False):
                # header
                col.write(field_name)

            for mapping in mappings_dict:
                (
                    col_obj_type,
                    col_obj_name,
                    col_attr_name,
                    col_time_series,
                    col_latest_datapoint,
                    col_retrieve,
                    col_aggr,
                    col_trans,
                    col_source,
                ) = st.columns(col_widths)
                col_obj_type.write(mapping.get("objectType"))
                col_obj_name.write(mapping.get("objectName"))
                col_attr_name.write(mapping.get("attributeName"))
                ts_ext_id = nested_get(mapping, ["timeSeries", "externalId"])
                transformations = mapping.get("transformations")
                if ts_ext_id in latest_ts_values_dict:
                    col_time_series.write(ts_ext_id)
                    dp = latest_ts_values_dict[ts_ext_id]
                    col_latest_datapoint.write(f"{ms_to_datetime(dp[0]).strftime('%Y-%m-%d %H:%M')}: {dp[1]}")
                elif ts_ext_id:
                    col_time_series.warning(f"{ts_ext_id} not found")
                elif transformations is None or len(transformations) == 0:
                    col_time_series.warning("Missing")
                else:
                    col_time_series.write(None)

                col_retrieve.write(mapping.get("retrieve"))
                col_aggr.write(mapping.get("aggregation"))
                if transformations:
                    col_trans.json(transformations, expanded=False)
                else:
                    col_trans.write(None)
                col_source.write(mapping.get("source"))

        else:
            st.text("No scenario loaded yet")
    else:
        st.write("Please select scenario first")
