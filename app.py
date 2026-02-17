import traceback
import streamlit as st

try:
    import polars as pl
    import plotly.express as px
except Exception as e:
    st.error("Import failed:")
    st.code(traceback.format_exc())
    st.stop()


import streamlit as st
import polars as pl
import plotly.express as px
import io
import requests


# 7. Dashboard Structure
st.set_page_config(
    page_title="NYC Taxi Dashboard ; COMP3610 Assignment 1",
    page_icon="taxi",
    layout="wide",
)

st.title("NYC Yellow Taxi Trips - January 2024")
st.write("This dashboard provides insights into NYC yellow taxi trips for January 2024 using interactive filters and visualisations, based on the data provided. The data includes trip details such as pickup and dropoff times, locations, fare amounts, and payment types.")

tab_overview, tab_charts = st.tabs(["Overview", "Charts"])

CLEANED_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/cleaned_taxi.parquet"
ZONES_PATH = "https://huggingface.co/datasets/Denelle/streamlit-data/resolve/main/zones.parquet"

PAYMENT_TYPE_LABELS = {
    1 : "Credit card",
    2 : "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
}

# @st.cache_data
# def load_cleaned_trips() -> pl.DataFrame:
#     return pl.read_parquet(CLEANED_PATH)

# @st.cache_data
# def load_zones() -> pl.DataFrame:
#     return pl.read_parquet(ZONES_PATH)

@st.cache_data(show_spinner="Downloading trip data (first run only)...")
def load_cleaned_trips() -> pl.DataFrame:
    r = requests.get(CLEANED_PATH, timeout=60)
    r.raise_for_status()
    return pl.read_parquet(io.BytesIO(r.content))

@st.cache_data(show_spinner="Downloading zones data (first run only)...")
def load_zones() -> pl.DataFrame:
    r = requests.get(ZONES_PATH, timeout=60)
    r.raise_for_status()
    return pl.read_parquet(io.BytesIO(r.content))

@st.cache_data
def ensure_zone_names(df: pl.DataFrame) -> pl.DataFrame:
    if "pickup_zone" in df.columns and "dropoff_zone" in df.columns:
        return df
    
    zones = load_zones()

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

    return (
        df
        .join(z_pick, left_on="PULocationID", right_on="LocationID", how="left")
        .join(z_drop, left_on="DOLocationID", right_on="LocationID", how="left")
    )

try:
    df = load_cleaned_trips()
except Exception as e:
    st.error(f"Error loading cleaned trips data: {e}")
    st.stop()

try:
    df = ensure_zone_names(df)
except Exception as e:
    st.error(f"Loaded cleaned dataset, but could not attach zone names: {e}")

# Interactive filters

st.sidebar.header("Filters")

# Date range filter
min_dt = df.select(pl.col("tpep_pickup_datetime").min()).item()
max_dt = df.select(pl.col("tpep_pickup_datetime").max()).item()

start_date, end_date = st.sidebar.date_input(
    "Date range",
    value=(min_dt.date(), max_dt.date()),
    min_value=min_dt.date(),
    max_value=max_dt.date(),
)

# Hour range filter
hour_min, hour_max = st.sidebar.slider(
    "Hour range (0-23)",
    0, 23, (0,23)
)

# Payemnt type filter
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
    #control how options are displayed to users
    format_func=lambda x: f"{int(x)} - {PAYMENT_TYPE_LABELS.get(int(x), 'Other')}"
)

# Apply filters
filtered = (
    df
    .filter(pl.col("tpep_pickup_datetime").dt.date().is_between(start_date, end_date))
    .filter(pl.col("pickup_hour").is_between(hour_min, hour_max))
    .filter(pl.col("payment_type").is_in(selected_pay))
)

if filtered.height == 0:
    st.warning("No trips match your filters.")
    st.stop()

# Key Metrics Display 
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

    st.markdown(
        "These metrics summarize the filtered subset of trips and provide quick context before exploring the charts."
    )

    # Required Visualisations 
with tab_charts:
        st.subheader("Visualisations")

        # Chart 1: Bar Chart - Top 10 Pickup Zones 
        group_col = "pickup_zone" if "pickup_zone" in filtered.columns else "PULocationID"

        top10_pickups = (
            filtered
            .group_by(group_col)
            .agg(pl.count().alias("trip_count"))
            .sort("trip_count", descending=True)
            .head(10)
        ).to_pandas()

        fig1 = px.bar(
            top10_pickups,
            x=group_col,
            y="trip_count",
            title="Top 10 Pickup Zones by Trip Count",
            labels={group_col: "Pickup Zone", "trip_count": "Trips"},
        )
        fig1.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig1, use_container_width=True)
        st.write(
        "**Insight:** Taxi demand is heavily concentrated in Midtown, the Upper East Side, and major airports, highlighting the importance of business districts and travel hubs in generating trip volume. This suggests strong commuter and tourism-driven mobility patterns across these zones."
        )

        # Chart 2: Line - Avergae fare by hour of day
        avg_fare_by_hour = (
            filtered
            .group_by("pickup_hour")
            .agg(pl.col("fare_amount").mean().alias("avg_fare"))
            .sort("pickup_hour")
        ).to_pandas()

        fig2 = px.line(
            avg_fare_by_hour,
            x="pickup_hour",
            y="avg_fare",
            markers=True,
            title="Average Fare by Hour of Day",
            labels={"pickup_hour": "Hour of Day", "avg_fare": "Average Fare ($)"}
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.write("**Insight:** The highest average fares occur in the early morning, particularly around 5 AM, likely driven by airport or long-distance trips. During most daytime hours, fares remain relatively stable between approximately $17–$19, with a modest increase again in the late evening as travel demand rises.")

        # Chart 3: Histogram - Trip Distance Distribution 
        dist_pdf = filtered.select("trip_distance").to_pandas()

        fig3 = px.histogram(
            dist_pdf,
            x="trip_distance",
            nbins=50,
            title="Distribution of Trip Distances",
            labels={"trip_distance": "Trip Distance (miles)"},
        )

        st.plotly_chart(fig3, use_container_width=True)
        st.write("**Insight:** Trip distances are heavily concentrated at the lower end of the scale, confirming that NYC taxi usage is dominated by short, intra-city travel. The pronounced right tail reflects occasional long-distance trips, such as airport or inter-borough journeys, but these occur far less frequently than short urban rides.")

        #Chart 4: Bar chart - Payment types breakdown
        payment_breakdown = (
            filtered
            .group_by("payment_type")
            .agg(pl.len().alias("trip_count"))
            .with_columns(
                pl.col("payment_type")
                    .cast(pl.Int32)
                    .map_elements(lambda x: f"{x} - {PAYMENT_TYPE_LABELS.get(x, 'Other')}") #maps payment type codes to readable labels
                    .alias("payment_label")
            )
            .sort("trip_count", descending=True)
        ).to_pandas()

        fig4 = px.bar(
            payment_breakdown,
            x="payment_label",
            y="trip_count",
            title="Payment Type Breakdown",
            labels={"payment_label": "Payment Type", "trip_count": "Number of Trips"},
        )
        fig4.update_traces(texttemplate="%{y:,}", textposition="outside")
        st.plotly_chart(fig4, use_container_width=True)
        st.write(
        "**Insight:** Credit card payments overwhelmingly dominate taxi transactions, accounting for the vast majority of trips, while cash represents a much smaller share. Other payment categories, including disputes and no-charge trips, contribute only a negligible portion of total rides. This distribution highlights the strong reliance on electronic payment systems in urban taxi operations."
        )

        # Chart 5: Heatmap - Trips by day of week and hour
        heat = (
            filtered
            .group_by(["pickup_day_of_week", "pickup_hour"])
            .agg(pl.count().alias("trip_count"))
        ).to_pandas()

        # Make day order readable 
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        heat["pickup_day_of_week"] = heat["pickup_day_of_week"].astype("category")
        heat["pickup_day_of_week"] = heat["pickup_day_of_week"].cat.set_categories(day_order, ordered=True)

        fig5 = px.density_heatmap(
            heat,
            x="pickup_hour",
            y="pickup_day_of_week",
            z="trip_count",
            title="Trips by Day of Week and Hour of Day",
            labels={"pickup_hour": "Hour of Day", "pickup_day_of_week":"Day of Week", "trip_count": "Number of Trips"},
        )
        st.plotly_chart(fig5, use_container_width=True)
        st.write(
        "**Insight:** Credit card payments overwhelmingly dominate taxi transactions, accounting for the vast majority of trips, while cash represents a much smaller share. Other payment categories, including disputes and no-charge trips, contribute only a negligible portion of total rides. This distribution highlights the strong reliance on electronic payment systems in urban taxi operations."
    )