"""
AIS Maritime Logistics Analytics Dashboard

Interactive Streamlit dashboard for exploring AIS pipeline output.
Reads directly from parquet gold/silver layers.

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import pydeck as pdk
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="AIS Maritime Analytics",
    page_icon="ship",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
GOLD_DIR = DATA_DIR / "gold"
SILVER_DIR = DATA_DIR / "silver"


# ---------------------------------------------------------------------------
# Load helpers (cached)
# ---------------------------------------------------------------------------
@st.cache(ttl=600, allow_output_mutation=True, show_spinner=True)
def load_activity() -> pd.DataFrame:
    df = pq.read_table(str(GOLD_DIR / "vessel_daily_activity")).to_pandas()
    df["activity_date"] = pd.to_datetime(df["activity_date"]).dt.date
    return df


@st.cache(ttl=600, allow_output_mutation=True, show_spinner=True)
def load_voyages() -> pd.DataFrame:
    df = pq.read_table(str(GOLD_DIR / "voyage_candidates")).to_pandas()
    return df


@st.cache(ttl=600, allow_output_mutation=True, show_spinner=True)
def load_routes() -> pd.DataFrame:
    df = pq.read_table(str(GOLD_DIR / "route_metrics")).to_pandas()
    df["metric_date"] = pd.to_datetime(df["metric_date"]).dt.date
    return df


@st.cache(ttl=600, allow_output_mutation=True, show_spinner=True)
def load_metadata() -> pd.DataFrame:
    return pq.read_table(str(GOLD_DIR / "vessel_metadata")).to_pandas()


@st.cache(ttl=600, allow_output_mutation=True, show_spinner=True)
def load_silver_sample(n: int = 200_000) -> pd.DataFrame:
    df = pq.read_table(
        str(SILVER_DIR),
        columns=["mmsi", "latitude", "longitude", "sog", "vessel_type", "event_time", "source_date"],
    ).to_pandas()
    if len(df) > n:
        df = df.sample(n=n, random_state=42)
    return df


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
activity = load_activity()
voyages = load_voyages()
routes = load_routes()
metadata = load_metadata()
positions = load_silver_sample()

# Merge vessel names into activity
activity_enriched = activity.merge(
    metadata[["mmsi", "vessel_name", "vessel_type"]],
    on="mmsi",
    how="left",
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.title("Filters")

# Date filter
available_dates = sorted(activity["activity_date"].unique())
selected_dates = st.sidebar.multiselect(
    "Activity Date",
    options=available_dates,
    default=available_dates,
)

# Route type filter
route_types = sorted(voyages["candidate_route_type"].dropna().unique())
selected_route_types = st.sidebar.multiselect(
    "Route Type",
    options=route_types,
    default=route_types,
)

# MMSI search
mmsi_search = st.sidebar.text_input("Search MMSI", placeholder="e.g. 311297000")

# Vessel name search
name_search = st.sidebar.text_input("Search Vessel Name", placeholder="e.g. DOLE")

# Speed filter
sog_range = st.sidebar.slider(
    "Avg SOG Range (knots)",
    min_value=0.0,
    max_value=float(activity["avg_sog"].max()),
    value=(0.0, float(activity["avg_sog"].max())),
    step=0.5,
)

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
filtered_activity = activity_enriched[
    (activity_enriched["activity_date"].isin(selected_dates))
    & (activity_enriched["avg_sog"].between(sog_range[0], sog_range[1]))
]

filtered_voyages = voyages[
    voyages["candidate_route_type"].isin(selected_route_types)
]

if mmsi_search.strip():
    try:
        mmsi_val = int(mmsi_search.strip())
        filtered_activity = filtered_activity[filtered_activity["mmsi"] == mmsi_val]
        filtered_voyages = filtered_voyages[filtered_voyages["mmsi"] == mmsi_val]
    except ValueError:
        st.sidebar.warning("MMSI must be a number")

if name_search.strip():
    mask = filtered_activity["vessel_name"].fillna("").str.contains(
        name_search.strip(), case=False
    )
    filtered_activity = filtered_activity[mask]
    matched_mmsis = filtered_activity["mmsi"].unique()
    filtered_voyages = filtered_voyages[filtered_voyages["mmsi"].isin(matched_mmsis)]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("AIS Maritime Logistics Analytics")
st.caption("Real-time insights from AIS vessel traffic data | Bronze > Silver > Gold pipeline")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric("Unique Vessels", f"{metadata['mmsi'].nunique():,}")
with kpi2:
    st.metric("Voyage Segments", f"{len(filtered_voyages):,}")
with kpi3:
    st.metric("Daily Records", f"{len(filtered_activity):,}")
with kpi4:
    total_positions = activity["point_count"].sum()
    st.metric("Total Positions", f"{total_positions:,}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 1: Speed distribution + Activity by vessel type
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Speed Distribution (Avg SOG)")
    if not filtered_activity.empty:
        speed_hist = filtered_activity["avg_sog"].dropna()
        bins = pd.cut(speed_hist, bins=20)
        speed_counts = bins.value_counts().sort_index().reset_index()
        speed_counts.columns = ["Speed Range", "Count"]
        speed_counts["Speed Range"] = speed_counts["Speed Range"].astype(str)
        st.bar_chart(speed_counts.set_index("Speed Range"))
    else:
        st.info("No data for selected filters")

with col2:
    st.subheader("Route Type Breakdown")
    if not filtered_voyages.empty:
        route_counts = filtered_voyages["candidate_route_type"].value_counts().reset_index()
        route_counts.columns = ["Route Type", "Voyages"]
        st.bar_chart(route_counts.set_index("Route Type"))
    else:
        st.info("No voyage data for selected filters")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 2: Vessel Position Map (Heatmap + Scatter)
# ---------------------------------------------------------------------------
st.subheader("Vessel Position Map")

map_col1, map_col2 = st.columns([4, 1])

with map_col2:
    map_mode = st.radio("Map Mode", ["Heatmap", "Points"], index=0)
    map_sample_size = st.slider("Map Points", 10000, 200000, 80000, step=10000)
    if map_mode == "Points":
        color_by = st.selectbox("Color By", ["Speed (SOG)", "Vessel Type"])
    else:
        color_by = None

map_data = positions.dropna(subset=["latitude", "longitude"]).copy()
if len(map_data) > map_sample_size:
    map_data = map_data.sample(n=map_sample_size, random_state=42)
map_data["sog"] = map_data["sog"].fillna(0)
map_data["vessel_type"] = map_data["vessel_type"].fillna(0).astype(int)

with map_col1:
    if not map_data.empty:
        view_state = pdk.ViewState(
            latitude=map_data["latitude"].median(),
            longitude=map_data["longitude"].median(),
            zoom=4,
            pitch=35 if map_mode == "Heatmap" else 0,
        )

        if map_mode == "Heatmap":
            # Heatmap - shows density clearly
            heat_layer = pdk.Layer(
                "HeatmapLayer",
                data=map_data,
                get_position=["longitude", "latitude"],
                get_weight="sog",
                radiusPixels=40,
                intensity=1,
                threshold=0.05,
                opacity=0.8,
            )
            deck = pdk.Deck(
                layers=[heat_layer],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/dark-v10",
            )
        else:
            # Scatter points with bright, high-contrast colors
            if color_by == "Speed (SOG)":
                max_sog = max(map_data["sog"].quantile(0.95), 1)
                ratio = (map_data["sog"].clip(0, max_sog) / max_sog)
                # Blue(slow) -> Cyan -> Yellow -> Red(fast)
                map_data["r"] = (ratio * 255).astype(int)
                map_data["g"] = ((1 - (ratio - 0.5).abs() * 2).clip(0, 1) * 255).astype(int)
                map_data["b"] = ((1 - ratio) * 255).astype(int)
            else:
                type_colors = {
                    30: [50, 150, 255],   # Fishing - bright blue
                    31: [50, 150, 255],
                    32: [50, 150, 255],
                    36: [255, 180, 40],   # Sailing - bright orange
                    37: [255, 180, 40],
                    52: [50, 220, 100],   # Tug - bright green
                    60: [200, 80, 255],   # Passenger - bright purple
                    61: [200, 80, 255],
                    70: [255, 60, 60],    # Cargo - bright red
                    71: [255, 60, 60],
                    72: [255, 60, 60],
                    80: [255, 230, 50],   # Tanker - bright yellow
                    81: [255, 230, 50],
                    89: [255, 230, 50],
                }
                default_color = [180, 180, 180]
                map_data["r"] = map_data["vessel_type"].map(lambda x: type_colors.get(int(x), default_color)[0])
                map_data["g"] = map_data["vessel_type"].map(lambda x: type_colors.get(int(x), default_color)[1])
                map_data["b"] = map_data["vessel_type"].map(lambda x: type_colors.get(int(x), default_color)[2])

            map_data["a"] = 220

            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position=["longitude", "latitude"],
                get_fill_color=["r", "g", "b", "a"],
                get_radius=5000,
                radius_min_pixels=2,
                radius_max_pixels=8,
                pickable=True,
            )
            deck = pdk.Deck(
                layers=[scatter_layer],
                initial_view_state=view_state,
                tooltip={"text": "MMSI: {mmsi}\nSOG: {sog} kn\nType: {vessel_type}"},
                map_style="mapbox://styles/mapbox/dark-v10",
            )

        st.pydeck_chart(deck)
    else:
        st.info("No position data available")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 3: Route Metrics table
# ---------------------------------------------------------------------------
st.subheader("Route Metrics Summary")

filtered_routes = routes[routes["candidate_route_type"].isin(selected_route_types)]

if not filtered_routes.empty:
    route_display = filtered_routes[[
        "metric_date", "candidate_route_type", "vessel_count",
        "avg_duration_hours", "avg_speed", "point_count",
    ]].sort_values(["metric_date", "vessel_count"], ascending=[True, False])

    route_display.columns = [
        "Date", "Route Type", "Vessels", "Avg Duration (h)", "Avg Speed (kn)", "Points"
    ]
    st.dataframe(route_display)
else:
    st.info("No route metrics for selected filters")

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 4: Top vessels + Voyage table
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Top Active Vessels", "Voyage Candidates", "Vessel Lookup"])

with tab1:
    if not filtered_activity.empty:
        top_vessels = (
            filtered_activity
            .groupby(["mmsi", "vessel_name"])
            .agg(
                total_points=("point_count", "sum"),
                avg_speed=("avg_sog", "mean"),
                max_speed=("max_sog", "max"),
                days_active=("activity_date", "nunique"),
            )
            .round(2)
            .sort_values("total_points", ascending=False)
            .head(50)
            .reset_index()
        )
        top_vessels.columns = [
            "MMSI", "Vessel Name", "Total Points", "Avg Speed (kn)",
            "Max Speed (kn)", "Days Active",
        ]
        st.dataframe(top_vessels)
    else:
        st.info("No activity data for selected filters")

with tab2:
    if not filtered_voyages.empty:
        voyage_display = (
            filtered_voyages
            .sort_values("duration_hours", ascending=False)
            .head(100)
            [[
                "mmsi", "start_time", "end_time", "duration_hours",
                "avg_sog", "candidate_route_type", "point_count",
                "start_latitude", "start_longitude",
                "end_latitude", "end_longitude",
            ]]
        )
        voyage_display.columns = [
            "MMSI", "Start", "End", "Duration (h)",
            "Avg SOG (kn)", "Route Type", "Points",
            "Start Lat", "Start Lon", "End Lat", "End Lon",
        ]
        st.dataframe(voyage_display)
    else:
        st.info("No voyage data for selected filters")

with tab3:
    st.write("Enter an MMSI to view detailed vessel information")
    lookup_mmsi = st.text_input("MMSI", key="lookup_mmsi", placeholder="e.g. 311297000")
    if lookup_mmsi.strip():
        try:
            mmsi_val = int(lookup_mmsi.strip())

            # Metadata
            vessel_info = metadata[metadata["mmsi"] == mmsi_val]
            if not vessel_info.empty:
                st.write("**Vessel Info**")
                info = vessel_info.iloc[0]
                info_col1, info_col2, info_col3 = st.columns(3)
                with info_col1:
                    st.write(f"**Name:** {info.get('vessel_name', 'N/A')}")
                    st.write(f"**IMO:** {info.get('imo', 'N/A')}")
                    st.write(f"**Call Sign:** {info.get('call_sign', 'N/A')}")
                with info_col2:
                    st.write(f"**Type:** {info.get('vessel_type', 'N/A')}")
                    st.write(f"**Length:** {info.get('length', 'N/A')} m")
                    st.write(f"**Width:** {info.get('width', 'N/A')} m")
                with info_col3:
                    st.write(f"**Transceiver:** {info.get('transceiver', 'N/A')}")
            else:
                st.warning("Vessel not found in metadata")

            # Activity
            vessel_activity = activity_enriched[activity_enriched["mmsi"] == mmsi_val]
            if not vessel_activity.empty:
                st.write("**Daily Activity**")
                st.dataframe(
                    vessel_activity[[
                        "activity_date", "point_count", "avg_sog", "max_sog",
                        "first_seen_time", "last_seen_time",
                    ]].sort_values("activity_date"),
                )

            # Voyages
            vessel_voyages = voyages[voyages["mmsi"] == mmsi_val]
            if not vessel_voyages.empty:
                st.write("**Voyages**")
                st.dataframe(
                    vessel_voyages[[
                        "start_time", "end_time", "duration_hours",
                        "avg_sog", "candidate_route_type",
                    ]].sort_values("start_time"),
                )

            # Track map
            vessel_positions = positions[positions["mmsi"] == mmsi_val]
            if not vessel_positions.empty:
                st.write("**Track Map**")
                track_view = pdk.ViewState(
                    latitude=vessel_positions["latitude"].median(),
                    longitude=vessel_positions["longitude"].median(),
                    zoom=6,
                    pitch=0,
                )
                vp_clean = vessel_positions.sort_values("event_time").fillna(0)
                track_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=vp_clean,
                    get_position=["longitude", "latitude"],
                    get_fill_color=[0, 200, 255, 200],
                    get_radius=2000,
                    radius_min_pixels=2,
                    radius_max_pixels=6,
                    pickable=True,
                )
                path_coords = vp_clean[["longitude", "latitude"]].values.tolist()
                path_layer = pdk.Layer(
                    "PathLayer",
                    data=pd.DataFrame([{"path": path_coords}]),
                    get_path="path",
                    get_color=[0, 200, 255, 150],
                    width_min_pixels=2,
                    get_width=3,
                )
                track_deck = pdk.Deck(
                    layers=[track_layer, path_layer],
                    initial_view_state=track_view,
                    tooltip={"text": "SOG: {sog} kn"},
                    map_style="mapbox://styles/mapbox/dark-v10",
                )
                st.pydeck_chart(track_deck)

        except ValueError:
            st.error("Please enter a valid numeric MMSI")

# ---------------------------------------------------------------------------
# Row 5: Voyage origin-destination map
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("Voyage Origins & Destinations")

arc_col1, arc_col2 = st.columns([4, 1])

with arc_col2:
    arc_route_filter = st.multiselect(
        "Show Routes",
        options=["transit", "coastal", "short_move"],
        default=["transit", "coastal"],
        key="arc_routes",
    )
    arc_limit = st.slider("Max Arcs", 500, 5000, 2000, step=500)
    min_duration = st.slider("Min Duration (h)", 0.0, 24.0, 1.0, step=0.5)

if not filtered_voyages.empty:
    # Filter: only moving voyages, skip stationary (start == end)
    voy_sample = filtered_voyages.dropna(
        subset=["start_latitude", "start_longitude", "end_latitude", "end_longitude"]
    )
    voy_sample = voy_sample[
        (voy_sample["candidate_route_type"].isin(arc_route_filter))
        & (voy_sample["duration_hours"] >= min_duration)
    ]

    if len(voy_sample) > arc_limit:
        voy_sample = voy_sample.sample(n=arc_limit, random_state=42)

    arc_data = voy_sample[[
        "mmsi", "start_latitude", "start_longitude",
        "end_latitude", "end_longitude",
        "candidate_route_type", "duration_hours", "avg_sog",
    ]].copy().fillna(0)

    # Bright, high-contrast colors per route type
    arc_colors = {
        "transit": [255, 70, 70],     # bright red
        "coastal": [50, 200, 255],    # bright cyan
        "short_move": [255, 200, 50], # bright yellow
    }
    arc_data["source_color"] = arc_data["candidate_route_type"].map(
        lambda x: arc_colors.get(x, [180, 180, 180])
    )
    arc_data["target_color"] = arc_data["candidate_route_type"].map(
        lambda x: [c // 2 for c in arc_colors.get(x, [90, 90, 90])]
    )

    with arc_col1:
        if not arc_data.empty:
            arc_layer = pdk.Layer(
                "ArcLayer",
                data=arc_data,
                get_source_position=["start_longitude", "start_latitude"],
                get_target_position=["end_longitude", "end_latitude"],
                get_source_color="source_color",
                get_target_color="target_color",
                get_width=2,
                pickable=True,
            )

            arc_view = pdk.ViewState(
                latitude=arc_data["start_latitude"].median(),
                longitude=arc_data["start_longitude"].median(),
                zoom=4,
                pitch=45,
            )

            arc_deck = pdk.Deck(
                layers=[arc_layer],
                initial_view_state=arc_view,
                tooltip={"text": "MMSI: {mmsi}\nType: {candidate_route_type}\nDuration: {duration_hours}h\nSpeed: {avg_sog} kn"},
                map_style="mapbox://styles/mapbox/dark-v10",
            )
            st.pydeck_chart(arc_deck)
        else:
            st.info("No voyages match filters")

    # Legend (outside columns to avoid nesting)
    if not arc_data.empty:
        st.markdown("**Legend:**  Transit = bright red  |  Coastal = cyan  |  Short Move = yellow")
else:
    with arc_col1:
        st.info("No voyage data available")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    f"Data: {len(available_dates)} days | "
    f"{metadata['mmsi'].nunique():,} vessels | "
    f"{activity['point_count'].sum():,} AIS positions | "
    "Pipeline: Bronze > Silver > Gold"
)
