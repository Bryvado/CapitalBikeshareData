#!/usr/bin/env python3
"""
analysis_interactive.py
-----------------------
Generates a self-contained interactive HTML data profile for Capital Bikeshare
trip data, with per-year census-tract choropleth maps built from pygris
boundaries.

Usage:
    python analysis_interactive.py [--root ROOT] [--output OUTPUT]
               [--tract-year YEAR] [--sample N]

Inputs (auto-detected — local or S3):
    manifest.csv                  – processing manifest from the R pipeline
    data/master/old_era.parquet   – pre-coordinate era trips
    data/master/new_era.parquet   – modern era trips

Output:
    OUTPUT (default: _site/interactive.html) – self-contained HTML dashboard

Environment variables (S3 mode):
    CBS_S3_BUCKET   – S3 bucket name
    CBS_S3_PREFIX   – optional key prefix (e.g. "cbs/prod")
    AWS_REGION      – AWS region (default "us-east-1")
"""

import argparse
import json
import os
import sys
from datetime import date
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import pyarrow.parquet as pq
import pygris
from shapely.geometry import box

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVICE_STATES = [("11", "DC"), ("24", "MD"), ("51", "VA")]
SERVICE_BBOX = {"xmin": -77.6, "xmax": -76.8, "ymin": 38.7, "ymax": 39.2}
SAMPLE_PER_ERA = 500_000
TRACT_YEAR = 2020

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# ---------------------------------------------------------------------------
# S3 / local helpers
# ---------------------------------------------------------------------------


def _use_s3():
    return bool(os.environ.get("CBS_S3_BUCKET", "").strip())


def _s3_client():
    import boto3
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def _s3_bucket():
    return os.environ.get("CBS_S3_BUCKET", "")


def _s3_key(path):
    prefix = os.environ.get("CBS_S3_PREFIX", "").strip("/")
    base = (prefix + "/") if prefix else ""
    return base + path.lstrip("/")


def _read_csv_s3(key):
    s3 = _s3_client()
    resp = s3.get_object(Bucket=_s3_bucket(), Key=key)
    return pd.read_csv(BytesIO(resp["Body"].read()))


def _read_parquet_s3(key):
    s3 = _s3_client()
    resp = s3.get_object(Bucket=_s3_bucket(), Key=key)
    return pq.read_table(BytesIO(resp["Body"].read())).to_pandas()


def _list_s3_keys(prefix):
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=_s3_bucket(), Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_manifest(root="."):
    """Return the processing manifest as a DataFrame; empty if absent."""
    if _use_s3():
        key = _s3_key("manifest.csv")
        try:
            return _read_csv_s3(key)
        except Exception as exc:
            print(f"  WARNING: could not read manifest from S3: {exc}", flush=True)
            return pd.DataFrame()

    path = Path(root) / "manifest.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def read_master(era, root="."):
    """Return the master parquet for *era* as a DataFrame; None if absent."""
    if _use_s3():
        # Try per-month partition files first (new format)
        prefix = _s3_key(f"data/master/{era}/")
        keys = [k for k in _list_s3_keys(prefix) if k.endswith(".parquet")]
        if keys:
            parts = []
            for k in keys:
                try:
                    parts.append(_read_parquet_s3(k))
                except Exception:
                    pass
            if parts:
                return pd.concat(parts, ignore_index=True)
        # Fall back to legacy monolithic file
        mono_key = _s3_key(f"data/master/{era}_era.parquet")
        try:
            return _read_parquet_s3(mono_key)
        except Exception:
            return None

    master_dir = Path(root) / "data" / "master"
    partition_dir = master_dir / era
    partition_paths = sorted(partition_dir.glob("*.parquet")) if partition_dir.exists() else []
    partitions = []
    for p in partition_paths:
        try:
            partitions.append(pq.read_table(str(p)).to_pandas())
        except Exception:
            pass

    mono_path = master_dir / f"{era}_era.parquet"
    mono = None
    if mono_path.exists():
        try:
            mono = pq.read_table(str(mono_path)).to_pandas()
        except Exception:
            mono = None

    if mono is not None and partitions and "source_file" in mono.columns:
        partition_sources = []
        for part in partitions:
            if "source_file" in part.columns:
                partition_sources.extend(part["source_file"].dropna().astype(str).unique().tolist())
        if partition_sources:
            mono = mono[~mono["source_file"].astype(str).isin(set(partition_sources))]

    pieces = ([] if mono is None else [mono]) + partitions
    if not pieces:
        return None
    return pd.concat(pieces, ignore_index=True)


# ---------------------------------------------------------------------------
# Data availability summary
# ---------------------------------------------------------------------------


def summarize_availability(root="."):
    """Return DataFrame with columns: year, month, n_trips, era."""
    mf = read_manifest(root)
    if mf.empty or "status" not in mf.columns:
        return pd.DataFrame(columns=["year", "month", "n_trips", "era"])

    ok = mf[mf["status"] == "ok"].copy()
    if ok.empty:
        return pd.DataFrame(columns=["year", "month", "n_trips", "era"])

    # Derive period label from filename (basename without extension/url path)
    ok["period"] = (
        ok["filename"]
        .str.split("/")
        .str[-1]
        .str.replace(r"-capitalbikeshare-tripdata\.zip$", "", regex=True)
    )
    ok["year"] = ok["period"].apply(
        lambda p: int(p[:4]) if len(p) in (4, 6) else None
    )
    ok["month"] = ok["period"].apply(
        lambda p: None if len(p) == 4 else (int(p[4:6]) if len(p) == 6 else None)
    )
    ok["n_trips"] = (
        pd.to_numeric(ok.get("rows", pd.Series(dtype=int)), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    avail = (
        ok[["year", "month", "n_trips", "era"]]
        .dropna(subset=["year", "month"])
        .copy()
    )

    # Expand annual periods (YYYY labels, primarily 2010–2017) into true
    # per-month counts using master parquet timestamps so the interactive
    # availability heatmap matches the static report.
    annual_rows = ok[ok["period"].str.len() == 4][["year", "era", "n_trips"]].dropna(subset=["year", "era"]).copy()
    if not annual_rows.empty:
        expanded_parts = []
        for era_val, era_annual in annual_rows.groupby("era"):
            years_needed = sorted(era_annual["year"].astype(int).unique().tolist())
            master = read_master(era_val, root)
            if master is None or master.empty or "started_at" not in master.columns:
                fallback = era_annual.copy()
                fallback["month"] = 1
                expanded_parts.append(fallback[["year", "month", "n_trips", "era"]])
                continue

            started = pd.to_datetime(master["started_at"], utc=True, errors="coerce")
            monthly_counts = (
                pd.DataFrame({"started_at": started})
                .dropna()
                .assign(
                    year=lambda d: d["started_at"].dt.year.astype(int),
                    month=lambda d: d["started_at"].dt.month.astype(int),
                )
            )
            monthly_counts = monthly_counts[monthly_counts["year"].isin(years_needed)]
            if monthly_counts.empty:
                fallback = era_annual.copy()
                fallback["month"] = 1
                expanded_parts.append(fallback[["year", "month", "n_trips", "era"]])
                continue

            monthly_counts = (
                monthly_counts.groupby(["year", "month"])
                .size()
                .reset_index(name="n_trips")
            )
            monthly_counts["era"] = era_val
            expanded_parts.append(monthly_counts[["year", "month", "n_trips", "era"]])

        if expanded_parts:
            avail = pd.concat([avail] + expanded_parts, ignore_index=True)

    avail["year"] = avail["year"].astype(int)
    avail["month"] = avail["month"].astype(int)
    avail["n_trips"] = pd.to_numeric(avail["n_trips"], errors="coerce").fillna(0).astype(int)
    avail = (
        avail.groupby(["year", "month", "era"], as_index=False)["n_trips"]
        .sum()
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )
    return avail


# ---------------------------------------------------------------------------
# Census tracts via pygris
# ---------------------------------------------------------------------------


def download_census_tracts(year=TRACT_YEAR, bbox=None, cache=True):
    """
    Download DC/MD/VA census tracts clipped to *bbox* using pygris.

    Returns a GeoDataFrame in EPSG:4326.
    """
    if bbox is None:
        bbox = SERVICE_BBOX
    bbox_geom = box(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"])

    gdfs = []
    for fips, name in SERVICE_STATES:
        print(f"  Downloading census tracts for {name} (year={year})…", flush=True)
        try:
            gdf = pygris.tracts(state=fips, year=year, cb=True, cache=cache)
            gdf = gdf.to_crs(epsg=4326)
            gdf = gdf[gdf.geometry.intersects(bbox_geom)].copy()
            gdfs.append(gdf)
        except Exception as exc:
            print(f"  WARNING: could not download tracts for {name}: {exc}", flush=True)

    if not gdfs:
        raise RuntimeError(
            "Failed to download census tracts for any state in the service area."
        )
    return pd.concat(gdfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Spatial trip-start aggregation
# ---------------------------------------------------------------------------


def _load_trip_coords(root, sample):
    """Load start-lat/lng + year from master parquets; return a single DataFrame."""
    parts = []
    for era in ("old", "new"):
        df = read_master(era, root)
        if df is None or df.empty:
            continue
        need = [c for c in ("started_at", "start_lat", "start_lng") if c in df.columns]
        if len(need) < 3:
            continue
        df = df[list(need)].dropna(subset=["start_lat", "start_lng"]).copy()
        df["started_at"] = pd.to_datetime(df["started_at"], utc=True, errors="coerce")
        df["year"] = df["started_at"].dt.year
        if sample and len(df) > sample:
            df = df.sample(sample, random_state=42)
        parts.append(df[["year", "start_lat", "start_lng"]])

    if not parts:
        return pd.DataFrame(columns=["year", "start_lat", "start_lng"])

    trips = pd.concat(parts, ignore_index=True)
    # Clip to service area bounding box
    bb = SERVICE_BBOX
    trips = trips[
        (trips["start_lat"] >= bb["ymin"]) & (trips["start_lat"] <= bb["ymax"])
        & (trips["start_lng"] >= bb["xmin"]) & (trips["start_lng"] <= bb["xmax"])
    ]
    return trips


def aggregate_trips_per_year(tracts_gdf, root=".", years=None, sample=SAMPLE_PER_ERA):
    """
    Count bikeshare trip starts per census tract for each year.

    Returns a dict {year: GeoDataFrame-with-n_trips}.
    Performs a single spatial join across all years for efficiency.
    """
    trips = _load_trip_coords(root, sample)

    if trips.empty:
        return {yr: tracts_gdf.assign(n_trips=0) for yr in (years or [])}

    if years is None:
        years = sorted(trips["year"].dropna().astype(int).unique().tolist())

    trips_gdf = gpd.GeoDataFrame(
        trips,
        geometry=gpd.points_from_xy(trips["start_lng"], trips["start_lat"]),
        crs="EPSG:4326",
    )

    # Single spatial join for all years
    print(
        f"  Spatially joining {len(trips_gdf):,} trip starts to "
        f"{len(tracts_gdf):,} tracts…",
        flush=True,
    )
    joined = gpd.sjoin(
        trips_gdf,
        tracts_gdf[["GEOID", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    result = {}
    for yr in years:
        counts = (
            joined[joined["year"] == yr]
            .groupby("GEOID")
            .size()
            .reset_index(name="n_trips")
        )
        gdf = tracts_gdf.merge(counts, on="GEOID", how="left")
        gdf["n_trips"] = gdf["n_trips"].fillna(0).astype(int)
        result[yr] = gdf

    return result


# ---------------------------------------------------------------------------
# Plotly figures
# ---------------------------------------------------------------------------


def build_availability_heatmap(avail):
    """Plotly heatmap: year (y-axis) × month (x-axis) = n_trips."""
    if avail.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No processed data found. Run the pipeline first.",
            showarrow=False,
            font={"size": 16},
        )
        fig.update_layout(title="Data Availability")
        return fig

    years = sorted(avail["year"].unique())
    months = list(range(1, 13))

    z, text = [], []
    for yr in years:
        row_z, row_t = [], []
        for mo in months:
            sub = avail[(avail["year"] == yr) & (avail["month"] == mo)]
            if sub.empty:
                row_z.append(None)
                row_t.append("")
            else:
                n = int(sub["n_trips"].iloc[0])
                row_z.append(n)
                row_t.append(f"{n:,}")
        z.append(row_z)
        text.append(row_t)

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=MONTH_NAMES,
            y=[str(yr) for yr in years],
            text=text,
            texttemplate="%{text}",
            colorscale="Plasma",
            colorbar={"title": "Trips"},
            hoverongaps=False,
        )
    )
    fig.update_layout(
        title="Capital Bikeshare — Data Availability by Year & Month",
        xaxis_title="Month",
        yaxis_title="Year",
        yaxis={"type": "category"},
        height=max(300, 40 * len(years) + 100),
        margin={"l": 60, "r": 40, "t": 60, "b": 60},
    )
    return fig


def build_choropleth_with_dropdown(yearly_tracts):
    """
    Plotly choropleth map of trip-start density per census tract.

    Uses a year dropdown to switch between years.
    *yearly_tracts*: dict {year: GeoDataFrame with n_trips}.
    """
    years = sorted(yearly_tracts.keys())
    if not years:
        fig = go.Figure()
        fig.add_annotation(
            text="No trip data available for mapping.",
            showarrow=False,
            font={"size": 16},
        )
        fig.update_layout(title="Trip-Start Density by Census Tract")
        return fig

    # Build shared GeoJSON once (same tract boundaries for all years)
    first_gdf = yearly_tracts[years[0]]
    geojson_data = json.loads(first_gdf[["GEOID", "geometry"]].to_json())

    traces = []
    buttons = []
    for i, yr in enumerate(years):
        gdf = yearly_tracts[yr]
        zmax = gdf["n_trips"].quantile(0.99) if gdf["n_trips"].max() > 0 else 1

        trace = go.Choroplethmapbox(
            geojson=geojson_data,
            locations=gdf["GEOID"].tolist(),
            featureidkey="properties.GEOID",
            z=gdf["n_trips"].tolist(),
            colorscale="Inferno",
            zmin=0,
            zmax=float(zmax),
            marker_opacity=0.75,
            marker_line_width=0.3,
            colorbar={"title": "Trip Starts"},
            name=str(yr),
            visible=(i == 0),
        )
        traces.append(trace)

        vis = [False] * len(years)
        vis[i] = True
        buttons.append(
            {
                "method": "update",
                "label": str(yr),
                "args": [
                    {"visible": vis},
                    {"title": f"Capital Bikeshare — Trip Starts by Census Tract ({yr})"},
                ],
            }
        )

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=f"Capital Bikeshare — Trip Starts by Census Tract ({years[0]})",
        mapbox={
            "style": "carto-positron",
            "center": {"lat": 38.9, "lon": -77.03},
            "zoom": 10,
        },
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.01,
                "xanchor": "left",
                "y": 1.12,
                "yanchor": "top",
                "bgcolor": "white",
                "bordercolor": "grey",
                "font": {"size": 13},
            }
        ],
        height=650,
        margin={"r": 0, "t": 80, "l": 0, "b": 0},
    )
    return fig


def build_monthly_bar_with_dropdown(avail):
    """
    Plotly bar chart of trips per month.

    Uses a year dropdown to switch between years.
    """
    if avail.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No availability data.", showarrow=False, font={"size": 16}
        )
        fig.update_layout(title="Monthly Trip Volume")
        return fig

    years = sorted(avail["year"].unique())
    months = list(range(1, 13))

    traces = []
    buttons = []
    for i, yr in enumerate(years):
        sub = avail[avail["year"] == yr].set_index("month")
        y_vals = [
            int(sub.loc[mo, "n_trips"]) if mo in sub.index else 0
            for mo in months
        ]
        traces.append(
            go.Bar(
                x=MONTH_NAMES,
                y=y_vals,
                name=str(yr),
                visible=(i == 0),
                marker_color="#4C78A8",
            )
        )
        vis = [False] * len(years)
        vis[i] = True
        buttons.append(
            {
                "method": "update",
                "label": str(yr),
                "args": [
                    {"visible": vis},
                    {"title": f"Monthly Trip Volume — {yr}"},
                ],
            }
        )

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=f"Monthly Trip Volume — {years[0]}",
        xaxis_title="Month",
        yaxis_title="Trips",
        yaxis_tickformat=",",
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.01,
                "xanchor": "left",
                "y": 1.15,
                "yanchor": "top",
                "bgcolor": "white",
                "bordercolor": "grey",
                "font": {"size": 13},
            }
        ],
        height=420,
        margin={"l": 60, "r": 40, "t": 80, "b": 60},
    )
    return fig


def build_yearly_stats_table(avail):
    """Plotly table of per-year trip totals."""
    if avail.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available.", showarrow=False, font={"size": 14}
        )
        fig.update_layout(title="Annual Summary")
        return fig

    yearly = (
        avail.groupby("year")
        .agg(months_available=("month", "count"), total_trips=("n_trips", "sum"))
        .reset_index()
        .sort_values("year")
    )

    # Alternating row colours
    n = len(yearly)
    row_colors = [["white" if i % 2 == 0 else "#f5f5f5" for i in range(n)]] * 3

    fig = go.Figure(
        go.Table(
            header={
                "values": ["<b>Year</b>", "<b>Periods Available</b>", "<b>Total Trips</b>"],
                "fill_color": "#2c3e50",
                "font": {"color": "white", "size": 13},
                "align": "center",
            },
            cells={
                "values": [
                    yearly["year"].tolist(),
                    yearly["months_available"].tolist(),
                    [f"{v:,}" for v in yearly["total_trips"].tolist()],
                ],
                "fill_color": row_colors,
                "align": "center",
                "font": {"size": 12},
            },
        )
    )
    fig.update_layout(
        title="Annual Trip Totals (processed periods only)",
        height=max(250, 36 * n + 90),
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return fig


# ---------------------------------------------------------------------------
# HTML assembly
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Capital Bikeshare — Interactive Data Profile</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      margin: 0; background: #f0f2f5; color: #222;
    }}
    header {{
      background: #2c3e50; color: white;
      padding: 22px 40px;
    }}
    header h1 {{ margin: 0 0 4px; font-size: 1.75rem; }}
    header p  {{ margin: 0; opacity: 0.72; font-size: 0.88rem; }}
    header a  {{ color: #7fb3d3; }}
    main {{
      max-width: 1200px; margin: 0 auto; padding: 32px 20px;
    }}
    section {{
      background: white; border-radius: 8px; padding: 22px 22px 12px;
      margin-bottom: 26px; box-shadow: 0 1px 5px rgba(0,0,0,.09);
    }}
    section h2 {{
      margin-top: 0; color: #2c3e50;
      border-bottom: 2px solid #ecf0f1; padding-bottom: 8px;
      font-size: 1.15rem;
    }}
    .note {{
      color: #888; font-size: 0.83rem; margin: 6px 0 4px;
      line-height: 1.5;
    }}
    footer {{
      text-align: center; padding: 18px; color: #aaa; font-size: 0.78rem;
    }}
    footer a {{ color: #2980b9; }}
    .plotly-graph-div {{ width: 100% !important; }}
  </style>
</head>
<body>
<header>
  <h1>Capital Bikeshare — Interactive Data Profile</h1>
  <p>
    Generated {generated_date} &nbsp;·&nbsp;
    Census tract boundaries via
    <a href="https://github.com/walker-data/pygris" target="_blank">pygris</a>
    &nbsp;·&nbsp;
    <a href="index.html">Static report ↗</a>
  </p>
</header>

<main>
  <section id="availability">
    <h2>Data Availability</h2>
    {heatmap_html}
    <p class="note">
      Color intensity = trip count per period. Grey cells = no processed data.
      Annual files (2010–2017) appear in the January column.
    </p>
  </section>

  <section id="map">
    <h2>Trip-Start Density by Census Tract</h2>
    {choropleth_html}
    <p class="note">
      Select a year from the dropdown. Color = trip-start count per tract.
      Cartographic-boundary shapefiles downloaded via
      <a href="https://github.com/walker-data/pygris" target="_blank">pygris</a>
      / US Census Bureau (year {tract_year}).
    </p>
  </section>

  <section id="monthly">
    <h2>Monthly Trip Volume</h2>
    {monthly_html}
    <p class="note">Select a year to view its monthly breakdown.</p>
  </section>

  <section id="stats">
    <h2>Annual Summary</h2>
    {stats_html}
  </section>
</main>

<footer>
  Source:
  <a href="https://capitalbikeshare.com/system-data" target="_blank">
    Capital Bikeshare open data</a> ·
  <a href="https://github.com/Bryvado/CapitalBikeshareData" target="_blank">
    Bryvado/CapitalBikeshareData</a>
</footer>
</body>
</html>
"""


def _fig_html(fig, first=False):
    """Serialize a plotly figure to an HTML div string."""
    return fig.to_html(
        full_html=False,
        include_plotlyjs="cdn" if first else False,
        config={"responsive": True, "displayModeBar": True},
    )


def build_html(heatmap, choropleth, monthly, stats, tract_year, generated_date):
    """Assemble and return the complete HTML page string."""
    return _HTML_TEMPLATE.format(
        generated_date=generated_date,
        tract_year=tract_year,
        heatmap_html=_fig_html(heatmap, first=True),
        choropleth_html=_fig_html(choropleth),
        monthly_html=_fig_html(monthly),
        stats_html=_fig_html(stats),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Generate an interactive Capital Bikeshare data profile HTML page."
    )
    parser.add_argument(
        "--root", default=".", help="Project root directory (default: .)"
    )
    parser.add_argument(
        "--output",
        default="_site/interactive.html",
        help="Output HTML file path (default: _site/interactive.html)",
    )
    parser.add_argument(
        "--tract-year",
        type=int,
        default=TRACT_YEAR,
        help=f"Census tract boundary vintage year (default: {TRACT_YEAR})",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=SAMPLE_PER_ERA,
        help=(
            f"Max trip rows per era for spatial aggregation "
            f"(default: {SAMPLE_PER_ERA:,}; 0 = all rows)"
        ),
    )
    args = parser.parse_args()

    sample = args.sample if args.sample > 0 else None
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("=== Capital Bikeshare Interactive Profile ===", flush=True)
    print(f"Root: {args.root}  |  Output: {out_path}", flush=True)
    if _use_s3():
        print(f"S3 mode: s3://{_s3_bucket()}", flush=True)

    # ── Step 1: Data availability ─────────────────────────────────────────
    print("\n[1/4] Summarising data availability…", flush=True)
    avail = summarize_availability(args.root)
    if avail.empty:
        print(
            "  WARNING: No processed data found — "
            "dashboard will display placeholder content.",
            flush=True,
        )
    else:
        years_range = f"{avail['year'].min()}–{avail['year'].max()}"
        print(f"  {len(avail)} period(s), years {years_range}", flush=True)

    # ── Step 2: Census tract boundaries via pygris ────────────────────────
    print("\n[2/4] Downloading census tract boundaries via pygris…", flush=True)
    tracts_gdf = None
    try:
        tracts_gdf = download_census_tracts(year=args.tract_year)
        print(f"  {len(tracts_gdf)} tracts in service area", flush=True)
    except Exception as exc:
        print(f"  ERROR: {exc}\n  Choropleth map will be empty.", flush=True)

    # ── Step 3: Per-year trip aggregation ─────────────────────────────────
    print("\n[3/4] Aggregating trip starts to census tracts per year…", flush=True)
    yearly_tracts = {}
    if tracts_gdf is not None and not avail.empty:
        years = sorted(avail["year"].unique().tolist())
        try:
            yearly_tracts = aggregate_trips_per_year(
                tracts_gdf, root=args.root, years=years, sample=sample
            )
            print(
                f"  Aggregated data for {len(yearly_tracts)} year(s): "
                + ", ".join(str(y) for y in sorted(yearly_tracts)),
                flush=True,
            )
        except Exception as exc:
            print(f"  WARNING: Spatial aggregation failed: {exc}", flush=True)

    # ── Step 4: Build figures and write HTML ──────────────────────────────
    print("\n[4/4] Building interactive figures and writing HTML…", flush=True)
    heatmap = build_availability_heatmap(avail)
    choropleth = build_choropleth_with_dropdown(yearly_tracts)
    monthly = build_monthly_bar_with_dropdown(avail)
    stats = build_yearly_stats_table(avail)

    generated_date = date.today().strftime("%B %d, %Y")
    html_content = build_html(
        heatmap, choropleth, monthly, stats, args.tract_year, generated_date
    )

    out_path.write_text(html_content, encoding="utf-8")
    size_kb = out_path.stat().st_size / 1024
    print(f"\n✓ Interactive profile written to {out_path} ({size_kb:.0f} KB)", flush=True)


if __name__ == "__main__":
    main()
