import os
import tempfile
import traceback
from datetime import date

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
    "Interactive dashboard for NYC yellow taxi trips (January 2024). "
    "Use the sidebar filters to explore trip patterns."
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
# Download once (cached) -> local parquet path
# -------------------------
@st.cache_data(show_spinner="Downloading parquet (first run only)...")
def download_parquet(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, stream=True, timeout=300)
    r.raise_for_status()

    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)

    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            if chunk:
                f.write(chunk)

    # parquet sanity check: header should be PAR1
    with open(path, "rb") as f:
        head = f.read(4)
    if head != b"PAR1":
        raise ValueError("Downloaded file is not a parquet file (missing PAR1 header).")

    return path


@st.cache_data(show_spinner=False)
def trips_lazy() -> pl.LazyFrame:
    """Lazy scan so we don't load the whole dataset into memory."""
    path = download_parquet(CLEANED_URL)
    return pl.scan_parquet(path)


@st.cache_data(show_spinner=False)
def zones_df() -> pl.DataFrame:
    """Zones is small; load as a normal DataFrame."""
    path = download_parquet(ZONES_URL)
    return pl.read_parquet(path)


# -------------------------
# Prevent Streamlit Cloud health-check timeout
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
# Sidebar filters (DON'T compute min/max from data; keep it fast)
# -------------------------
st.sidebar.header("Filters")

jan_start = date(2024, 1, 1)
jan_end = date(2024, 1, 31)

date_val = st.sidebar.date_input(
    "Pickup date range (January 2024)",
    value=(jan_start, jan_end),
    min_value=jan_start,
    max_value=jan_end,
)

# Streamlit can return single date, 1-item tuple, or 2-item tuple
if isinstance(date_val, (tuple, list)):
    if len(date_val) == 2:
        start_date, end_date = date_val
    elif len(date_val) == 1:
        start_date = end_date = date_val[0]
    else:
        start_date = end_date = jan_start
else:
    start_date = end_date = date_val

if start_date > end_date:
    start_date, end_date = end_date, start_date

hour_min, hour_max = st.sidebar.slider("Pickup hour range (0–23)", 0, 23, (0, 23))

# Don’t compute pay options from the dataset on every rerun (expensive).
# Use known TLC codes.
pay_opts = [1, 2, 3, 4, 5]
selected_pay = st.sidebar.multiselect(
    "Payment types",
    options=pay_opts,
    default=pay_opts,
    format_func=lambda x: f"{int(x)} - {PAYMENT_TYPE_LABELS.get(int(x), 'Other')}",
)

# -------------------------
# Filter lazily, then collect a smaller DataFrame
# -------------------------
try:
    with st.spinner("Filtering trips..."):
        lf = trips_lazy()

        filtered = (
            lf
            .filter(pl.col("tpep_pickup_datetime").dt.date().is_between(start_date, end_date))
            .filter(pl.col("pickup_hour").is_between(hour_min, hour_max))
            .filter(pl.col("payment_type").is_in(selected_pay))
            .select(NEEDED_COLS)
            .collect()
        )
except Exception:
    st.error("Crash while filtering trips:")
    st.code(traceback.format_exc())
    st.stop()

if filtered.height == 0:
    st.warning("No trips match your filters.")
    st.stop()

# Optional safety cap if filters still produce a massive subset (prevents Cloud OOM)
MAX_ROWS = 300_000
if filtered.height > MAX_ROWS:
    st.warning(f"Large result set ({filtered.height:,} rows). Showing first {MAX_ROWS:,} rows for stability.")
    filtered = filtered.head(MAX_ROWS)

# -------------------------
# Attach zone names ONLY on filtered subset (fast + memory-safe)
# -------------------------
try:
    with st.spinner("Attaching zone names..."):
        zones = zones_df()

        z_pick = zones.select([
            pl.col("LocationID"),
            pl.col("Zone").alias("pickup_zone"),
            pl.col("Borough").alias("pickup_borough"),
        ])
        z_drop = zones.select([
            pl.col("LocationID"),
            pl.col("Zone").alias("dropoff_zone"),
            pl.col("Borough").alias("dropoff_borough"),
        ])

        filtered = (
            filtered
            .join(z_pick, left_on="PULocationID", right_on="LocationID", how="left")
            .join(z_drop, left_on="DOLocationID", right_on="LocationID", how="left")
        )
except Exception:
    st.warning("Could not attach zone names (continuing without them).")
    st.code(traceback.format_exc())

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

    st.caption("Metrics reflect the filtered subset of trips.")

# -------------------------
# Charts tab
# -------------------------
with tab_charts:
    st.subheader("Visualisations")

    # Chart 1: Top 10 Pickup Zones
    group_col = "pickup_zone" if "pickup_zone" in filtered.columns else "PULocationID"
    top10_pickups = (
        filtered
        .group_by(group_col)
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
    st.write("Taxi demand is heavily concentrated in Midtown Center, the Upper East Side, and major airports, highlighting the importance of business districts and travel hubs in generating trips. This suggests strong commuter and tourism-driven mobility patterns across these zones. ")

    # Chart 2: Average Fare by Hour
    avg_fare_by_hour = (
        filtered
        .group_by("pickup_hour")
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
    st.write("The highest average fares occur in the early morning, particularly around 5 AM, likely driven by airport or long-distance trips. During most daytime hours, fares remain relatively stable between approximately $17–$19, with a modest increase again in the late evening as travel demand rises.")

    # Chart 3: Trip Distance Distribution
    dist_dict = filtered.select("trip_distance").to_dict(as_series=False)
    fig3 = px.histogram(
        dist_dict,
        x="trip_distance",
        nbins=50,
        title="Distribution of Trip Distances",
        labels={"trip_distance": "Trip Distance (miles)"},
    )
    st.plotly_chart(fig3, width="stretch")
    st.write("Trip distances are heavily concentrated at the lower end of the scale, confirming that NYC taxi usage is dominated by short, intra-city travel. The pronounced right tail reflects occasional long-distance trips, such as airport or inter-borough journeys, but these occur far less frequently than short urban rides.")

    # Chart 4: Payment Type Breakdown
    payment_breakdown = (
        filtered
        .group_by("payment_type")
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
    st.write("Credit card payments overwhelmingly dominate taxi transactions, accounting for the vast majority of trips, while cash represents a much smaller share. Other payment categories, including disputes and no-charge trips, contribute only a negligible portion of total rides. This distribution highlights the strong reliance on electronic payment systems in urban taxi operations.")

    # Chart 5: Heatmap (Day vs Hour)
    heat = (
        filtered
        .group_by(["pickup_day_of_week", "pickup_hour"])
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
    st.write("Trip volume peaks during weekday commuting hours, particularly between late afternoon and early evening (around 4 PM–7 PM), with Wednesday showing especially high demand. Early morning hours (approximately 2 AM–5 AM) consistently record the lowest trip counts across all days. On weekends, demand shifts later into the day, reflecting leisure and social travel patterns rather than traditional commuting activity.")
