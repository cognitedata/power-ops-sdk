from datetime import date, datetime, time

import pandas as pd
import streamlit as st
import yaml
from cognite.client import CogniteClient
from cognite.powerops.client import PowerOpsClient
from utils import filters_to_str, nested_get

######################################################################################################## UTILS
plotly_config = dict(fillFrame=True, displayModeBar=False, showTips=False)


common_layout = dict(
    xaxis=dict(
        title=dict(text=None),
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
    ),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    hovermode="x",
    hoverlabel=dict(namelength=-1),
    spikedistance=-1,
    margin=dict(l=70, r=20, b=30, t=50, pad=0),
    height=700,
)

custom_css = {
    "@font-face": {
        "font-family": '"Source Sans Pro"',
        "src": 'url(https://fonts.gstatic.com/s/sourcesanspro/v21/6xK3dSBYKcSV-LCoeQqfX1RYOo3qOK7lujVj9w.woff2) format("woff2")',
    }
}


def load_file_contents(file_external_id: str):
    try:
        file_contents = client.files.download_bytes(external_id=file_external_id)
        st.session_state["shop_model_dict"] = yaml.safe_load(file_contents)
        st.session_state["loaded_file"] = file_external_id
    except Exception as e:
        st.toast("Unable to load file contents as yaml. Error message: " + str(e))


def create_layout(title: str | None, y_axis_name: str | None) -> dict:
    extra_layout = {}
    if title:
        extra_layout = extra_layout | {"title": {"text": title}}
    if y_axis_name:
        extra_layout = extra_layout | {"yaxis": {"title": {"text": y_axis_name}}}
    return common_layout | extra_layout


def snake_to_camel(s):
    parts = s.split("_")
    return parts[0] + "".join(x.title() for x in parts[1:])


def timestamp_from_date_and_time(_date: date, _time: time) -> int:
    return int(datetime(_date.year, _date.month, _date.day, _time.hour, _time.minute).timestamp() * 1000)


def combine_date_and_time(_date: date, _time: time) -> datetime:
    return datetime.combine(_date, _time)
    # f"{_date.isoformat()} {_time.isoformat()}"


######################################################################################################## UTILS
client = CogniteClient()
po_client = PowerOpsClient(read_dataset="powerops:misc", write_dataset="powerops:misc", client=client) # TODO: remove the datasets from constructorx


################################################################################################# Page config
st.set_page_config(layout="wide", page_title="CogShop explorer")
st.title("Explore Shop results")

################################################################################################# TAB 1

url_params = st.query_params
if "file_external_id" in url_params:
    if "exp_result_selector_expanded" not in st.session_state:
        st.session_state["exp_result_selector_expanded"] = False
    if "exp_result_explorer_expanded" not in st.session_state:
        st.session_state["exp_result_explorer_expanded"] = True
    if "file_external_id" not in st.session_state:
        st.session_state["file_external_id"] = url_params["file_external_id"]
        load_file_contents(file_external_id=st.session_state["file_external_id"])
else:
    if "exp_result_selector_expanded" not in st.session_state:
        st.session_state["exp_result_selector_expanded"] = True
    if "exp_result_explorer_expanded" not in st.session_state:
        st.session_state["exp_result_explorer_expanded"] = True


def combine_date_and_time(_date: date, _time: time) -> datetime:
    return datetime.combine(_date, _time)


exp_result_selector = st.expander(
    expanded=st.session_state["exp_result_selector_expanded"],
    label="Find ShopResult instance",
)
exp_result_explorer = st.expander(
    expanded=st.session_state["exp_result_explorer_expanded"],
    label="Look at Shop results",
)


with exp_result_selector:
    col_filters, col_results = st.columns([2, 10])
    with col_filters:
        #################################################################################### Time series search options
        # scenario_xid_input: str = st.text_input("Scenario external ID", key="scenario_xid", value="")
        now = datetime.now()
        with st.popover(label="Start and end time filters", use_container_width=True):
            case_min_start_time_date_input = st.date_input(
                "Start time after date",
                datetime(now.year, 1, 1),
                format="YYYY-MM-DD",
            )
            case_min_start_time_input = st.time_input("... and time", time(hour=0, minute=0))
            case_max_start_time_date_input = st.date_input(
                "Start time before date",
                datetime(now.year + 1, 1, 1),
                format="YYYY-MM-DD",
            )
            case_max_start_time_input = st.time_input(
                "... and time",
                time(hour=23, minute=59),
            )
        # case_extid_input: str = st.text_input("Case external ID", key="case_xid")

        last_updated_after_input = st.text_input("Last updated after", now.strftime("%Y-%m-%dT00:00:00"))
        try:
            last_updated_after = datetime.fromisoformat(last_updated_after_input)
        except:
            last_updated_after = None
            st.warning("Date must be in iso format (YYYY-MM-DDThh:mm:ss[+zh:zm])")

        scenario_name_prefix_input = st.text_input(label="Scenario name prefix")
        model_name_prefix_input = st.text_input(label="Model name prefix")
        case_limit: int = st.number_input(label="Number of results", value=20, max_value=1000, min_value=1, step=1)

    with col_results:
        filters = [
            '{case: {startTime: {gte: "'
            + combine_date_and_time(case_min_start_time_date_input, case_min_start_time_input).isoformat()
            + '"}}}',
            '{case: {startTime: {lte: "'
            + combine_date_and_time(case_max_start_time_date_input, case_max_start_time_input).isoformat()
            + '"}}}',
        ]
        if last_updated_after:
            filters.append('{lastUpdatedTime: {gte: "' + last_updated_after.isoformat() + '"}}')
        if scenario_name_prefix_input:
            filters.append(
                '{case: {scenario: {name: {prefix: "' + scenario_name_prefix_input + '"}}}}',
            )
        if model_name_prefix_input:
            filters.append(
                '{case: {scenario: {model: {name: {prefix: "' + model_name_prefix_input + '"}}}}}',
            )
        filters_str = filters_to_str(filters)

        query = (
            """query ShopResult {
    listShopResult("""
            + filters_str
            + """ first: """
            + str(case_limit)
            + """) {
        items {
        lastUpdatedTime
        case {
            externalId
            startTime
            endTime
            scenario {
            externalId
            name
            model {
                externalId
                name
            }
            }
        }
        objectiveValue
        preRun {
            externalId
            downloadLink {
                downloadUrl
            }
        }
        postRun {
            externalId
            downloadLink {
                downloadUrl
            }
        }
        messages {
            externalId
            downloadLink {
                downloadUrl
            }
        }
        alerts(first: 1000) {
            items {
                severity
                title
                description
            }
        }
        }
    }
    }"""
        )
        # st.write(query)
        shop_results = client.data_modeling.graphql.query(("power_ops_core", "all_PowerOps", "1"), query)[
            "listShopResult"
        ]["items"]

        # st.write(shop_results)
        col_widths = (2, 2, 2, 2, 2, 1, 1, 1, 1)
        colms = st.columns(col_widths)
        fields = [
            "Model",
            "Scenario",
            "StartTime",
            "EndTime",
            "LastUpdated",
            "PostRun",
            "PreRun",
            "Files",
            "Alerts",
        ]
        for col, field_name in zip(colms, fields, strict=False):
            # header
            col.write(field_name)

        for ii in range(len(shop_results)):
            shop_result = shop_results[ii]
            col1, col2, col3, col4, col5, col6, col7, col_download, col_alerts = st.columns(col_widths)
            col1.write(nested_get(shop_result, ["case", "scenario", "model", "name"]))
            col2.write(nested_get(shop_result, ["case", "scenario", "name"]))
            col3.write(nested_get(shop_result, ["case", "startTime"]))
            col4.write(nested_get(shop_result, ["case", "endTime"]))
            col5.write(shop_result.get("lastUpdatedTime"))
            with col6:
                post_run_file_xid = nested_get(shop_result, ["postRun", "externalId"])
                if post_run_file_xid:
                    select_post_run = st.button("Select", key=f"postRun{ii}")
                    if select_post_run:
                        st.session_state["file_external_id"] = post_run_file_xid
                        st.session_state["exp_result_selector_expanded"] = False
                        st.session_state["exp_result_explorer_expanded"] = True

            with col7:
                pre_run_file_xid = nested_get(shop_result, ["preRun", "externalId"])
                if pre_run_file_xid:
                    select_post_run = st.button("Select", key=f"preRun{ii}")
                    if select_post_run:
                        st.session_state["file_external_id"] = pre_run_file_xid
                        st.session_state["exp_result_selector_expanded"] = False
                        st.session_state["exp_result_explorer_expanded"] = True
            with col_download:
                with st.popover(label=""):
                    for _shop_file in ["postRun", "preRun", "messages"]:
                        if url := ((shop_result.get(_shop_file) or {}).get("downloadLink", {}) or {}).get(
                            "downloadUrl", {}
                        ):
                            st.link_button(
                                label=f"Download {_shop_file}",
                                url=shop_result[_shop_file]["downloadLink"]["downloadUrl"],
                            )
                        else:
                            st.write(f"{_shop_file} not found")
            with col_alerts:
                if len(shop_result.get("alerts", {}).get("items", [])) > 0:
                    alerts = shop_result["alerts"]["items"]
                    with st.popover(label=str(len(alerts))):
                        for alert in alerts:
                            st.write(alert.get("title"))
                            st.write("Description")
                            st.write(alert.get("description"))
                            st.write()


# selected = {}


def selector_if_relevant(data, label):
    if isinstance(data, dict):
        first_key, first_value = next(iter(data.items()))
        if isinstance(first_key, datetime):  # Then we assume it is a time series, and will try to plot it
            df = pd.DataFrame(data, index=[label]).T
            st.write("Chart")
            st.line_chart(df)
            st.write("Table")
            st.table(df)
        else:
            selected = st.selectbox(options=data, label=f"Select within {label}")
            selector_if_relevant(data=data[selected], label=selected)
    else:
        st.write(data)


with exp_result_explorer:
    st.session_state["file_external_id"] = st.text_input(
        label="File external ID:", value=st.session_state.get("file_external_id")
    )
    if st.session_state.get("file_external_id"):
        load_file = st.button(
            "Load file contents",
            on_click=lambda: load_file_contents(st.session_state["file_external_id"]),
        )
    if "shop_model_dict" in st.session_state:
        if st.session_state.get("file_external_id") and st.session_state["file_external_id"] != st.session_state.get(
            "loaded_file"
        ):
            st.write(
                f"Still showing contents of file {st.session_state['loaded_file']} - not the specified one ({st.session_state['file_external_id']})"
            )
        else:
            st.write(f"Showing contents of file {st.session_state['loaded_file']}")
        shop_model_dict = st.session_state.shop_model_dict

        try:
            plants = shop_model_dict["model"]["plant"]
            if "production" in list(plants.values())[0]:
                attr = "production"
            elif "sim_production" in list(plants.values())[0]:
                attr = "sim_production"
            else:
                raise
            st.header(f"Plant {attr}")
            st.write("Production [MWh/h] per plant, stacked")
            st.bar_chart({plant_name: plant_data[attr] for plant_name, plant_data in plants.items()})
        except Exception:
            st.text("Not able to find/plot plant production (nor sim_production)")

        try:
            reservoirs = shop_model_dict["model"]["reservoir"]
            if "storage" in list(reservoirs.values())[0]:
                attr = "storage"
                descr = "storage [million m3]"
            elif "sim_inflow" in list(reservoirs.values())[0]:
                attr = "sim_inflow"
                descr = "inflow [m3/s]"
            else:
                raise

            st.header(f"Reservoir {attr}")
            if len(reservoirs) < 5:
                _default = list(reservoirs.keys())
            else:
                _default = []
            reservoirs_selected = st.multiselect(options=reservoirs, label="Select reservoirs", default=_default)
            for reservoir_name in reservoirs_selected:
                st.write(f"{reservoir_name} {descr}")
                reservoir_data = reservoirs[reservoir_name]
                ts = reservoir_data[attr]
                # TODO: Consider adding min and max limits to the chart
                df = pd.DataFrame(ts, index=[f"{reservoir_name} {attr}"]).T
                st.area_chart(df)
        except Exception:
            st.text("Not able to find/plot reservoir storage (nor sim_inflow)")

        st.header("Other data")
        selector_if_relevant(shop_model_dict, "whole file")
    else:
        st.text("No file loaded yet")
