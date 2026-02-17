# import io
# import traceback

# import requests
# import streamlit as st

# try:
#     import polars as pl
#     import plotly.express as px
# except Exception:
#     st.error("Import failed:")
#     st.code(traceback.format_exc())
#     st.stop()

# # 7. Dashboard Structure
# st.set_page_config(
#     page_title="NYC Taxi Dashboard ; COMP3610 Assignment 1",
#     page_icon="🚕",
#     layout="wide",
# )

# st.title("NYC Yellow Taxi Trips - January 2024")
# st.write("This dashboard provides insights into NYC yellow taxi trips for January 2024 using interactive filters and visualisations, based on the data provided. The data includes trip details such as pickup and dropoff times, locations, fare amounts, and payment types.")

# tab_overview, tab_charts = st.tabs(["Overview", "Charts"])

# CLEANED_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/cleaned_taxi.parquet?download=true"
# ZONES_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/zones.parquet?download=true"


# # CLEANED_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/cleaned_taxi.parquet"
# # ZONES_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/zones.parquet"

# PAYMENT_TYPE_LABELS = {
#     1 : "Credit card",
#     2 : "Cash",
#     3: "No charge",
#     4: "Dispute",
#     5: "Unknown",
# }

# # @st.cache_data
# # def load_cleaned_trips() -> pl.DataFrame:
# #     return pl.read_parquet(CLEANED_PATH)

# # @st.cache_data
# # def load_zones() -> pl.DataFrame:
# #     return pl.read_parquet(ZONES_PATH)

# @st.cache_data(show_spinner="Downloading trip data (first run only)...")
# def load_cleaned_trips() -> pl.DataFrame:
#     r = requests.get(CLEANED_PATH, timeout=60)
#     r.raise_for_status()
#     return pl.read_parquet(io.BytesIO(r.content))

# @st.cache_data(show_spinner="Downloading zones data (first run only)...")
# def load_zones() -> pl.DataFrame:
#     r = requests.get(ZONES_PATH, timeout=60)
#     r.raise_for_status()
#     return pl.read_parquet(io.BytesIO(r.content))

# @st.cache_data
# def ensure_zone_names(df: pl.DataFrame) -> pl.DataFrame:
#     if "pickup_zone" in df.columns and "dropoff_zone" in df.columns:
#         return df
    
#     zones = load_zones()

#     z_pick = zones.select([
#         pl.col("LocationID"),
#         pl.col("Zone").alias("pickup_zone"),
#         pl.col("Borough").alias("pickup_borough"),
#     ])

#     z_drop = zones.select([
#         pl.col("LocationID"),
#         pl.col("Zone").alias("dropoff_zone"),
#         pl.col("Borough").alias("dropoff_borough"),
#     ])

#     return (
#         df
#         .join(z_pick, left_on="PULocationID", right_on="LocationID", how="left")
#         .join(z_drop, left_on="DOLocationID", right_on="LocationID", how="left")
#     )

# if "ready" not in st.session_state:
#     st.session_state.ready = False

# if not st.session_state.ready:
#     st.info("Click to load the dataset (first run downloads the parquet).")
#     if st.button("Load data"):
#         st.session_state.ready = True
#         st.rerun()
#     st.stop()

# try:
#     df = load_cleaned_trips()
# except Exception as e:
#     st.error(f"Error loading cleaned trips data: {e}")
#     st.stop()

# try:
#     df = ensure_zone_names(df)
# except Exception as e:
#     st.error(f"Loaded cleaned dataset, but could not attach zone names: {e}")

# # Interactive filters

# st.sidebar.header("Filters")

# # Date range filter
# min_dt = df.select(pl.col("tpep_pickup_datetime").min()).item()
# max_dt = df.select(pl.col("tpep_pickup_datetime").max()).item()

# date_val = st.sidebar.date_input(
#     "Date range",
#     value=(min_dt.date(), max_dt.date()),
#     min_value=min_dt.date(),
#     max_value=max_dt.date(),
# )

# if isinstance(date_val, tuple):
#     start_date, end_date = date_val
# else:
#     start_date = end_date = date_val

# # Hour range filter
# hour_min, hour_max = st.sidebar.slider(
#     "Hour range (0-23)",
#     0, 23, (0,23)
# )

# # Payemnt type filter
# pay_opts = sorted(
#     df.select("payment_type")
#     .unique()
#     .to_series()
#     .drop_nulls()
#     .to_list()
# )
# selected_pay = st.sidebar.multiselect(
#     "Payment types",
#     options=pay_opts,
#     default=pay_opts,
#     #control how options are displayed to users
#     format_func=lambda x: f"{int(x)} - {PAYMENT_TYPE_LABELS.get(int(x), 'Other')}"
# )

# # Apply filters
# filtered = (
#     df
#     .filter(pl.col("tpep_pickup_datetime").dt.date().is_between(start_date, end_date))
#     .filter(pl.col("pickup_hour").is_between(hour_min, hour_max))
#     .filter(pl.col("payment_type").is_in(selected_pay))
# )

# if filtered.height == 0:
#     st.warning("No trips match your filters.")
#     st.stop()

# # Key Metrics Display 
# with tab_overview:
#     st.subheader("Key Metrics")

#     total_trips = filtered.height
#     avg_fare = filtered.select(pl.col("fare_amount").mean()).item()
#     total_revenue = filtered.select(pl.col("total_amount").sum()).item()
#     avg_trip_distance = filtered.select(pl.col("trip_distance").mean()).item()
#     avg_trip_duration = filtered.select(pl.col("trip_duration_minutes").mean()).item()

#     c1, c2, c3, c4, c5 = st.columns(5)
#     c1.metric("Total Trips", f"{total_trips:,}")
#     c2.metric("Average Fare ($)", f"${avg_fare:.2f}")
#     c3.metric("Total Revenue ($)", f"${total_revenue:.2f}")
#     c4.metric("Average Trip Distance (miles)", f"{avg_trip_distance:.2f} mi")
#     c5.metric("Average Trip Duration (minutes)", f"{avg_trip_duration:.2f} min")

#     st.markdown(
#         "These metrics summarize the filtered subset of trips and provide quick context before exploring the charts."
#     )

#     # Required Visualisations 
# with tab_charts:
#     st.subheader("Visualisations")

#     # Chart 1: Bar Chart - Top 10 Pickup Zones 
#     group_col = "pickup_zone" if "pickup_zone" in filtered.columns else "PULocationID"

#     top10_pickups = (
#         filtered
#         .group_by(group_col)
#         .agg(pl.count().alias("trip_count"))
#         .sort("trip_count", descending=True)
#         .head(10)
#     )

#     fig1 = px.bar(
#         top10_pickups,
#         x=group_col,
#         y="trip_count",
#         title="Top 10 Pickup Zones by Trip Count",
#         labels={group_col: "Pickup Zone", "trip_count": "Trips"},
#     )
#     fig1.update_layout(xaxis_tickangle=-30)
#     st.plotly_chart(fig1, use_container_width=True)
#     st.write(
#     "**Insight:** Taxi demand is heavily concentrated in Midtown, the Upper East Side, and major airports, highlighting the importance of business districts and travel hubs in generating trip volume. This suggests strong commuter and tourism-driven mobility patterns across these zones."
#     )

#     # Chart 2: Line - Avergae fare by hour of day
#     avg_fare_by_hour = (
#         filtered
#         .group_by("pickup_hour")
#         .agg(pl.col("fare_amount").mean().alias("avg_fare"))
#         .sort("pickup_hour")
#     )

#     fig2 = px.line(
#         avg_fare_by_hour,
#         x="pickup_hour",
#         y="avg_fare",
#         markers=True,
#         title="Average Fare by Hour of Day",
#         labels={"pickup_hour": "Hour of Day", "avg_fare": "Average Fare ($)"}
#     )
#     st.plotly_chart(fig2, use_container_width=True)
#     st.write("**Insight:** The highest average fares occur in the early morning, particularly around 5 AM, likely driven by airport or long-distance trips. During most daytime hours, fares remain relatively stable between approximately $17–$19, with a modest increase again in the late evening as travel demand rises.")

#     # Chart 3: Histogram - Trip Distance Distribution 
#     dist_pdf = filtered.select("trip_distance").to_pandas()

#     fig3 = px.histogram(
#         dist_pdf,
#         x="trip_distance",
#         nbins=50,
#         title="Distribution of Trip Distances",
#         labels={"trip_distance": "Trip Distance (miles)"},
#     )

#     st.plotly_chart(fig3, use_container_width=True)
#     st.write("**Insight:** Trip distances are heavily concentrated at the lower end of the scale, confirming that NYC taxi usage is dominated by short, intra-city travel. The pronounced right tail reflects occasional long-distance trips, such as airport or inter-borough journeys, but these occur far less frequently than short urban rides.")

#     #Chart 4: Bar chart - Payment types breakdown
#     payment_breakdown = (
#         filtered
#         .group_by("payment_type")
#         .agg(pl.len().alias("trip_count"))
#         .with_columns(
#             pl.col("payment_type")
#                 .cast(pl.Int32)
#                 .map_elements(lambda x: f"{x} - {PAYMENT_TYPE_LABELS.get(x, 'Other')}") #maps payment type codes to readable labels
#                 .alias("payment_label")
#         )
#         .sort("trip_count", descending=True)
#     )

#     fig4 = px.bar(
#         payment_breakdown,
#         x="payment_label",
#         y="trip_count",
#         title="Payment Type Breakdown",
#         labels={"payment_label": "Payment Type", "trip_count": "Number of Trips"},
#     )
#     fig4.update_traces(texttemplate="%{y:,}", textposition="outside")
#     st.plotly_chart(fig4, use_container_width=True)
#     st.write(
#     "**Insight:** Credit card payments overwhelmingly dominate taxi transactions, accounting for the vast majority of trips, while cash represents a much smaller share. Other payment categories, including disputes and no-charge trips, contribute only a negligible portion of total rides. This distribution highlights the strong reliance on electronic payment systems in urban taxi operations."
#     )

#     # Chart 5: Heatmap - Trips by day of week and hour
#     heat = (
#         filtered
#         .group_by(["pickup_day_of_week", "pickup_hour"])
#         .agg(pl.len().alias("trip_count"))
#     )

#     # Make day order readable 
#     day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
#     # heat["pickup_day_of_week"] = heat["pickup_day_of_week"].astype("category")
#     # heat["pickup_day_of_week"] = heat["pickup_day_of_week"].cat.set_categories(day_order, ordered=True)

#     fig5 = px.density_heatmap(
#         heat.to_dict(as_series=False),
#         x="pickup_hour",
#         y="pickup_day_of_week",
#         z="trip_count",
#         category_orders={"pickup_day_of_week": day_order},
#         title="Trips by Day of Week and Hour of Day",
#         labels={"pickup_hour": "Hour of Day", "pickup_day_of_week":"Day of Week", "trip_count": "Number of Trips"},
#     )
#     st.plotly_chart(fig5, use_container_width=True)
#     st.write(
#     "**Insight:** Credit card payments overwhelmingly dominate taxi transactions, accounting for the vast majority of trips, while cash represents a much smaller share. Other payment categories, including disputes and no-charge trips, contribute only a negligible portion of total rides. This distribution highlights the strong reliance on electronic payment systems in urban taxi operations."
#     )

import io
import traceback

import requests
import streamlit as st

try:
    import polars as pl
    import plotly.express as px
except Exception:
    st.error("Import failed:")
    st.code(traceback.format_exc())
    st.stop()

# -------------------------
# Page setup
# -------------------------
st.set_page_config(
    page_title="NYC Taxi Dashboard ; COMP3610 Assignment 1",
    page_icon="🚕",
    layout="wide",
)

st.title("NYC Yellow Taxi Trips - January 2024")
st.write(
    "This dashboard provides insights into NYC yellow taxi trips for January 2024 using interactive filters and visualisations, "
    "based on the data provided. The data includes trip details such as pickup and dropoff times, locations, fare amounts, and payment types."
)

tab_overview, tab_charts = st.tabs(["Overview", "Charts"])

CLEANED_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/cleaned_taxi.parquet?download=true"
ZONES_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/zones.parquet?download=true"

PAYMENT_TYPE_LABELS = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
}

NEEDED_COLS = [
    "tpep_pickup_datetime",
    "pickup_hour",
    "pickup_day_of_week",
    "trip_duration_minutes",
    "payment_type",
    "fare_amount",
    "total_amount",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]

# -------------------------
# Data loading helpers
# -------------------------
@st.cache_data(show_spinner="Downloading trip data (first run only)...")
def load_cleaned_trips() -> pl.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(CLEANED_PATH, headers=headers, allow_redirects=True, timeout=180)
    r.raise_for_status()

    # Parquet files start with magic bytes "PAR1"
    if not r.content.startswith(b"PAR1"):
        st.error("Downloaded trip file is not a parquet file (got HTML/redirect/etc).")
        st.code(r.text[:500] if hasattr(r, "text") else str(r.content[:200]))
        st.stop()

    return pl.read_parquet(io.BytesIO(r.content), columns=NEEDED_COLS)


@st.cache_data(show_spinner="Downloading zones data (first run only)...")
def load_zones() -> pl.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(ZONES_PATH, headers=headers, allow_redirects=True, timeout=180)
    r.raise_for_status()

    if not r.content.startswith(b"PAR1"):
        st.error("Downloaded zones file is not a parquet file (got HTML/redirect/etc).")
        st.code(r.text[:500] if hasattr(r, "text") else str(r.content[:200]))
        st.stop()

    return pl.read_parquet(io.BytesIO(r.content))


@st.cache_data
def ensure_zone_names(df: pl.DataFrame) -> pl.DataFrame:
    if "pickup_zone" in df.columns and "dropoff_zone" in df.columns:
        return df

    zones = load_zones()

    z_pick = zones.select(
        [
            pl.col("LocationID"),
            pl.col("Zone").alias("pickup_zone"),
            pl.col("Borough").alias("pickup_borough"),
        ]
    )

    z_drop = zones.select(
        [
            pl.col("LocationID"),
            pl.col("Zone").alias("dropoff_zone"),
            pl.col("Borough").alias("dropoff_borough"),
        ]
    )

    return (
        df.join(z_pick, left_on="PULocationID", right_on="LocationID", how="left")
          .join(z_drop, left_on="DOLocationID", right_on="LocationID", how="left")
    )

# -------------------------
# Prevent Cloud health-check timeout
# -------------------------
if "ready" not in st.session_state:
    st.session_state.ready = False

if not st.session_state.ready:
    st.info("Click to load the dataset (first run downloads the parquet).")
    if st.button("Load data"):
        st.session_state.ready = True
        st.rerun()
    st.stop()

# -------------------------
# Load data
# -------------------------
try:
    df = load_cleaned_trips()
except Exception:
    st.error("Error loading cleaned trips data:")
    st.code(traceback.format_exc())
    st.stop()

try:
    df = ensure_zone_names(df)
except Exception:
    st.warning("Loaded cleaned dataset, but could not attach zone names:")
    st.code(traceback.format_exc())

# -------------------------
# Sidebar filters
# -------------------------
st.sidebar.header("Filters")

# min_dt = df.select(pl.col("tpep_pickup_datetime").min()).item()
# max_dt = df.select(pl.col("tpep_pickup_datetime").max()).item()

# date_val = st.sidebar.date_input(
#     "Date range",
#     value=(min_dt.date(), max_dt.date()),
#     min_value=min_dt.date(),
#     max_value=max_dt.date(),
# )
# if isinstance(date_val, tuple):
#     start_date, end_date = date_val
# else:
#     start_date = end_date = date_val

min_dt = df.select(pl.col("tpep_pickup_datetime").min()).item()
max_dt = df.select(pl.col("tpep_pickup_datetime").max()).item()

date_val = st.sidebar.date_input(
    "Date range",
    value=(min_dt.date(), max_dt.date()),
    min_value=min_dt.date(),
    max_value=max_dt.date(),
)

# Streamlit can return:
# - a single date
# - a tuple/list with 1 date
# - a tuple/list with 2 dates
if isinstance(date_val, (tuple, list)):
    if len(date_val) == 2:
        start_date, end_date = date_val
    elif len(date_val) == 1:
        start_date = end_date = date_val[0]
    else:
        start_date = end_date = min_dt.date()
else:
    start_date = end_date = date_val

# (optional) ensure start <= end
if start_date > end_date:
    start_date, end_date = end_date, start_date


hour_min, hour_max = st.sidebar.slider("Hour range (0-23)", 0, 23, (0, 23))

pay_opts = sorted(
    df.select("payment_type")
      .unique()
      .to_series()
      .drop_nulls()
      .to_list()
)

selected_pay = st.sidebar.multiselect(
    "Payment types",
    options=pay_opts,
    default=pay_opts,
    format_func=lambda x: f"{int(x)} - {PAYMENT_TYPE_LABELS.get(int(x), 'Other')}",
)

# Apply filters
filtered = (
    df.filter(pl.col("tpep_pickup_datetime").dt.date().is_between(start_date, end_date))
      .filter(pl.col("pickup_hour").is_between(hour_min, hour_max))
      .filter(pl.col("payment_type").is_in(selected_pay))
)

if filtered.height == 0:
    st.warning("No trips match your filters.")
    st.stop()

# -------------------------
# Overview tab
# -------------------------
with tab_overview:
    st.subheader("Key Metrics")

    total_trips = filtered.height
    avg_fare = filtered.select(pl.col("fare_amount").mean()).item()
    total_revenue = filtered.select(pl.col("total_amount").sum()).item()
    avg_trip_distance = filtered.select(pl.col("trip_distance").mean()).item()
    avg_trip_duration = filtered.select(pl.col("trip_duration_minutes").mean()).item()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Trips", f"{total_trips:,}")
    c2.metric("Average Fare ($)", f"${avg_fare:.2f}")
    c3.metric("Total Revenue ($)", f"${total_revenue:.2f}")
    c4.metric("Average Trip Distance (miles)", f"{avg_trip_distance:.2f} mi")
    c5.metric("Average Trip Duration (minutes)", f"{avg_trip_duration:.2f} min")

    st.markdown("These metrics summarize the filtered subset of trips and provide quick context before exploring the charts.")

# -------------------------
# Charts tab
# -------------------------
with tab_charts:
    st.subheader("Visualisations")

    # Chart 1: Top 10 pickup zones
    group_col = "pickup_zone" if "pickup_zone" in filtered.columns else "PULocationID"
    top10_pickups = (
        filtered.group_by(group_col)
                .agg(pl.len().alias("trip_count"))
                .sort("trip_count", descending=True)
                .head(10)
    )
    fig1 = px.bar(
        top10_pickups.to_dict(as_series=False),
        x=group_col,
        y="trip_count",
        title="Top 10 Pickup Zones by Trip Count",
        labels={group_col: "Pickup Zone", "trip_count": "Trips"},
    )
    fig1.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig1, width="stretch")
    st.write(
        "**Insight:** Taxi demand is heavily concentrated in Midtown, the Upper East Side, and major airports, highlighting the importance of business districts and travel hubs in generating trip volume."
    )

    # Chart 2: Average fare by hour
    avg_fare_by_hour = (
        filtered.group_by("pickup_hour")
                .agg(pl.col("fare_amount").mean().alias("avg_fare"))
                .sort("pickup_hour")
    )
    fig2 = px.line(
        avg_fare_by_hour.to_dict(as_series=False),
        x="pickup_hour",
        y="avg_fare",
        markers=True,
        title="Average Fare by Hour of Day",
        labels={"pickup_hour": "Hour of Day", "avg_fare": "Average Fare ($)"},
    )
    st.plotly_chart(fig2, width="stretch")
    st.write(
        "**Insight:** The highest average fares occur in the early morning, likely driven by airport or long-distance trips."
    )

    # Chart 3: Trip distance distribution (NO pandas)
    dist_dict = filtered.select("trip_distance").to_dict(as_series=False)
    fig3 = px.histogram(
        dist_dict,
        x="trip_distance",
        nbins=50,
        title="Distribution of Trip Distances",
        labels={"trip_distance": "Trip Distance (miles)"},
    )
    st.plotly_chart(fig3, width="stretch")
    st.write(
        "**Insight:** Trip distances are heavily concentrated at the lower end, confirming NYC taxi usage is dominated by short, intra-city travel."
    )

    # Chart 4: Payment type breakdown
    payment_breakdown = (
        filtered.group_by("payment_type")
                .agg(pl.len().alias("trip_count"))
                .with_columns(
                    pl.col("payment_type")
                      .cast(pl.Int32)
                      .map_elements(lambda x: f"{x} - {PAYMENT_TYPE_LABELS.get(x, 'Other')}")
                      .alias("payment_label")
                )
                .sort("trip_count", descending=True)
    )
    fig4 = px.bar(
        payment_breakdown.to_dict(as_series=False),
        x="payment_label",
        y="trip_count",
        title="Payment Type Breakdown",
        labels={"payment_label": "Payment Type", "trip_count": "Number of Trips"},
    )
    fig4.update_traces(texttemplate="%{y:,}", textposition="outside")
    st.plotly_chart(fig4, width="stretch")
    st.write(
        "**Insight:** Credit card payments dominate taxi transactions, while cash represents a much smaller share."
    )

    # Chart 5: Heatmap trips by day/hour
    heat = (
        filtered.group_by(["pickup_day_of_week", "pickup_hour"])
                .agg(pl.len().alias("trip_count"))
    )
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    fig5 = px.density_heatmap(
        heat.to_dict(as_series=False),
        x="pickup_hour",
        y="pickup_day_of_week",
        z="trip_count",
        category_orders={"pickup_day_of_week": day_order},
        title="Trips by Day of Week and Hour of Day",
        labels={
            "pickup_hour": "Hour of Day",
            "pickup_day_of_week": "Day of Week",
            "trip_count": "Number of Trips",
        },
    )
    st.plotly_chart(fig5, width="stretch")
    st.write(
        "**Insight:** Taxi activity peaks on weekdays during commute hours and increases again on weekends in late evening periods."
    )
