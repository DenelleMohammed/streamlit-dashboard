import os
import tempfile
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
    "based on the data provided."
)

tab_overview, tab_charts = st.tabs(["Overview", "Charts"])

CLEANED_URL = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/cleaned_taxi.parquet?download=true"
ZONES_URL = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/zones.parquet?download=true"

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
# Download + load (cached)
# -------------------------
@st.cache_data(show_spinner="Downloading parquet (first run only)...")
def download_parquet(url: str) -> str:
    """
    Downloads url to a temp file and returns the file path.
    Cached so it won't download again on reruns / filter changes.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, stream=True, timeout=300)
    r.raise_for_status()

    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)

    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            if chunk:
                f.write(chunk)

    # quick parquet sanity check (magic bytes "PAR1")
    with open(path, "rb") as f:
        head = f.read(4)
    if head != b"PAR1":
        raise ValueError("Downloaded file is not a parquet file (missing PAR1 header).")

    return path


@st.cache_data(show_spinner="Loading trips…")
def load_trips() -> pl.DataFrame:
    path = download_parquet(CLEANED_URL)
    return pl.read_parquet(path, columns=NEEDED_COLS)


@st.cache_data(show_spinner="Loading zones…")
def load_zones() -> pl.DataFrame:
    path = download_parquet(ZONES_URL)
    return pl.read_parquet(path)


@st.cache_data(show_spinner="Attaching zone names…")
def trips_with_zones() -> pl.DataFrame:
    """
    Expensive join step cached so filters don't redo it.
    """
    df = load_trips()
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


@st.cache_data
def payment_options(df: pl.DataFrame):
    return sorted(
        df.select("payment_type").unique().to_series().drop_nulls().to_list()
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
# Load df once (cached)
# -------------------------
try:
    df = trips_with_zones()
except Exception:
    st.error("Crash while loading data:")
    st.code(traceback.format_exc())
    st.stop()

# -------------------------
# Sidebar filters
# -------------------------
st.sidebar.header("Filters")

min_dt = df.select(pl.col("tpep_pickup_datetime").min()).item()
max_dt = df.select(pl.col("tpep_pickup_datetime").max()).item()

date_val = st.sidebar.date_input(
    "Date range",
    value=(min_dt.date(), max_dt.date()),
    min_value=min_dt.date(),
    max_value=max_dt.date(),
)

# handle single date, 1-item tuple, 2-item tuple
if isinstance(date_val, (tuple, list)):
    if len(date_val) == 2:
        start_date, end_date = date_val
    elif len(date_val) == 1:
        start_date = end_date = date_val[0]
    else:
        start_date = end_date = min_dt.date()
else:
    start_date = end_date = date_val

if start_date > end_date:
    start_date, end_date = end_date, start_date

hour_min, hour_max = st.sidebar.slider("Hour range (0-23)", 0, 23, (0, 23))

pay_opts = payment_options(df)

selected_pay = st.sidebar.multiselect(
    "Payment types",
    options=pay_opts,
    default=pay_opts,
    format_func=lambda x: f"{int(x)} - {PAYMENT_TYPE_LABELS.get(int(x), 'Other')}",
)

# Apply filters (cheap compared to download/join)
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

    # Chart 3: Trip distance histogram (no pandas)
    dist_dict = filtered.select("trip_distance").to_dict(as_series=False)
    fig3 = px.histogram(
        dist_dict,
        x="trip_distance",
        nbins=50,
        title="Distribution of Trip Distances",
        labels={"trip_distance": "Trip Distance (miles)"},
    )
    st.plotly_chart(fig3, width="stretch")

    # Chart 4: Payment breakdown
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

    # Chart 5: Heatmap day vs hour
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