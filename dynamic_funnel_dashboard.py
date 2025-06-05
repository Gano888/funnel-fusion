import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.graph_objects as go
import re

# ──────────────────────────────────────────────────────────────────────────────
# 1) PAGE CONFIG & TITLE / BASIC INSTRUCTIONS
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("Internal Link Analysis")

st.markdown(
    """
    **How to use this app**:
    1. Upload a “Classification” CSV (must contain columns: `Address`, `Funnel`, `Topic`, `Geo`).
    2. Upload an “Inlinks” CSV (must contain columns: `Source`, `Destination`, `Anchor`, `Link Position`).
    3. Use the sidebar filters to slice by Funnel, Geo, and Link Position.
    4. View Link Gap Analysis or Funnel Flow in the tabs below.
    """
)

# ──────────────────────────────────────────────────────────────────────────────
# 2) HELPER: ROBUST URL NORMALIZATION
# ──────────────────────────────────────────────────────────────────────────────
def normalize_url(raw_url: str) -> str:
    """
    Strips whitespace, removes http:// or https:// prefixes, lowercases,
    and trims trailing slashes.
    """
    if pd.isna(raw_url):
        return ""
    u = raw_url.strip()
    u = re.sub(r"^https?://", "", u, flags=re.IGNORECASE)
    u = u.lower()
    u = u.rstrip("/")
    return u

# ──────────────────────────────────────────────────────────────────────────────
# 3) DUCKDB CONNECTION MANAGEMENT
# ──────────────────────────────────────────────────────────────────────────────
def get_duckdb():
    if "duckdb_conn" not in st.session_state:
        conn = duckdb.connect(database=":memory:")
        st.session_state.duckdb_conn = conn
    return st.session_state.duckdb_conn

# ──────────────────────────────────────────────────────────────────────────────
# 4) CACHING DISTINCT‐VALUES QUERIES
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_distinct_values(table_name: str, column: str) -> list[str]:
    """
    Executes: SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL
    and returns a sorted Python list of non-null values.
    """
    con = get_duckdb()
    try:
        rows = con.execute(
            f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL"
        ).fetchall()
        return sorted([r[0] for r in rows if r[0] is not None])
    except Exception:
        return []

# ──────────────────────────────────────────────────────────────────────────────
# 5) UTILITY: SAFE SQL‐IN LIST BUILDER
# ──────────────────────────────────────────────────────────────────────────────
def to_sql_str_list(items: list[str]) -> str:
    """
    Given a non-empty list of strings, returns a SQL‐safe tuple string:
      ["US","CA"] -> "('US','CA')"
    Caller must ensure `items` is not empty.
    """
    escaped = ["'" + str(i).replace("'", "''") + "'" for i in items]
    return "(" + ", ".join(escaped) + ")"

# ──────────────────────────────────────────────────────────────────────────────
# 6) FILE UPLOADS IN THE SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────
pages_file   = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV",   type="csv")

if pages_file and anchors_file:
    # ──────────────────────────────────────────────────────────────────────────
    # 6.1) READ & VALIDATE “PAGES” CSV (NO LONGER REQUIRES lat/lon)
    # ──────────────────────────────────────────────────────────────────────────
    try:
        pages_df_raw = pd.read_csv(
            io.StringIO(pages_file.read().decode("utf-8"))
        )
    except Exception as e:
        st.error(f"❌ Failed to read Classification CSV: {e}")
        st.stop()

    # Check required columns for pages_df_raw (no lat/lon needed anymore)
    required_pages = {"Address", "Funnel", "Topic", "Geo"}
    missing_pages = required_pages - set(pages_df_raw.columns)
    if missing_pages:
        st.error(
            f"❌ Your Classification CSV is missing the following column(s): "
            f"{', '.join(sorted(missing_pages))}"
        )
        st.stop()

    # Normalize the Address column into a new “URL” column
    pages_df_raw["URL"] = pages_df_raw["Address"].apply(normalize_url)

    # ──────────────────────────────────────────────────────────────────────────
    # 6.2) READ & VALIDATE “ANCHORS” CSV
    # ──────────────────────────────────────────────────────────────────────────
    try:
        anchors_df_raw = pd.read_csv(
            io.StringIO(anchors_file.read().decode("utf-8"))
        )
    except Exception as e:
        st.error(f"❌ Failed to read Inlinks CSV: {e}")
        st.stop()

    # Check required columns for anchors_df_raw
    required_anchors = {"Source", "Destination", "Anchor", "Link Position"}
    missing_anchors = required_anchors - set(anchors_df_raw.columns)
    if missing_anchors:
        st.error(
            f"❌ Your Inlinks CSV is missing the following column(s): "
            f"{', '.join(sorted(missing_anchors))}"
        )
        st.stop()

    # Normalize Source → FromURL, Destination → ToURL, and rename “Anchor” → “Anchor Text”
    anchors_df_raw["FromURL"] = anchors_df_raw["Source"].apply(normalize_url)
    anchors_df_raw["ToURL"]   = anchors_df_raw["Destination"].apply(normalize_url)
    anchors_df_raw = anchors_df_raw.rename(columns={"Anchor": "Anchor Text"})

    # ──────────────────────────────────────────────────────────────────────────
    # 6.3) LOAD (OR RELOAD) INTO DUCKDB
    # ──────────────────────────────────────────────────────────────────────────
    def load_tables(pages_df: pd.DataFrame, anchors_df: pd.DataFrame):
        con = get_duckdb()
        try:
            con.execute("DROP TABLE IF EXISTS pages;")
            con.execute("DROP TABLE IF EXISTS anchors;")
        except duckdb.Error:
            pass

        con.register("pages_view", pages_df)
        con.execute("CREATE TABLE pages AS SELECT * FROM pages_view;")

        con.register("anchors_view", anchors_df)
        con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view;")

        return con

    con = load_tables(pages_df_raw, anchors_df_raw)

    # ──────────────────────────────────────────────────────────────────────────
    # 7) BUILD FILTER VALUES (Funnel, Geo, Link Position) USING CACHED QUERIES
    # ──────────────────────────────────────────────────────────────────────────
    funnel_list   = get_distinct_values("pages", "Funnel")
    geo_list      = get_distinct_values("pages", "Geo")
    position_list = get_distinct_values("anchors", "\"Link Position\"")

    selected_funnels   = st.sidebar.multiselect("Funnel Stage(s)", funnel_list, default=funnel_list)
    selected_geos      = st.sidebar.multiselect("Geo(s)", geo_list, default=geo_list)
    selected_positions = st.sidebar.multiselect("Link Position(s)", position_list, default=position_list)

    # ──────────────────────────────────────────────────────────────────────────
    # 8) APPLY FILTERS & QUERY DUCKDB
    # ──────────────────────────────────────────────────────────────────────────
    if not selected_funnels or not selected_geos:
        # If either Funnel or Geo is completely deselected, force empty DataFrames
        pages_df   = pd.DataFrame(columns=pages_df_raw.columns.tolist() + ["URL"])
        anchors_df = pd.DataFrame(columns=anchors_df_raw.columns.tolist() + ["FromURL", "ToURL"])
    else:
        # 8.1) FILTER pages_df: only keep rows with Funnel ∈ selected_funnels AND Geo ∈ selected_geos
        pages_sql = f"""
            SELECT 
              *, 
              URL
            FROM pages
            WHERE Funnel IN {to_sql_str_list(selected_funnels)}
              AND Geo    IN {to_sql_str_list(selected_geos)}
        """
        try:
            pages_df = con.execute(pages_sql).fetchdf()
        except Exception as e:
            st.error(f"❌ Error running pages filter SQL: {e}")
            st.stop()

        # 8.2) FILTER anchors_df: only keep rows with Link Position ∈ selected_positions
        if not selected_positions:
            anchors_df = pd.DataFrame(columns=anchors_df_raw.columns.tolist() + ["FromURL", "ToURL"])
        else:
            anchors_sql = f"""
                SELECT 
                  *, 
                  FromURL, 
                  ToURL, 
                  "Anchor Text" 
                FROM anchors
                WHERE "Link Position" IN {to_sql_str_list(selected_positions)}
            """
            try:
                anchors_df = con.execute(anchors_sql).fetchdf()
            except Exception as e:
                st.error(f"❌ Error running anchors filter SQL: {e}")
                st.stop()

    # ──────────────────────────────────────────────────────────────────────────
    # 9) MAIN UI TABS (ONLY TWO NOW: Gap Analysis & Funnel Flow)
    # ──────────────────────────────────────────────────────────────────────────
    tabs = st.tabs(["🔍 Link Gap Analysis", "📊 Funnel Flow"])

    # ──────────────────────────────────────────────────────────────────────────
    # Tab 1: 🔍 Link Gap Analysis
    # ──────────────────────────────────────────────────────────────────────────
    with tabs[0]:
        if pages_df.empty:
            st.warning("No pages to display (check your Funnel/Geo selections).")
        else:
            # Compute how many inbound links each page has
            inbound_counts = (
                anchors_df.groupby("ToURL")["Anchor Text"]
                .count()
                .reset_index(name="InboundLinks")
            )
            gap_df = pages_df.merge(
                inbound_counts,
                left_on="URL",
                right_on="ToURL",
                how="left",
            )
            gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)

            max_links = int(gap_df["InboundLinks"].max()) if not gap_df.empty else 0
            threshold = st.slider(
                "Maximum Inbound Links",
                min_value=0,
                max_value=max_links,
                value=max_links,
            )

            filtered = gap_df[gap_df["InboundLinks"] <= threshold][
                ["URL", "Funnel", "Topic", "Geo", "InboundLinks"]
            ]

            st.dataframe(filtered)
            st.download_button(
                "📥 Download Gap Results",
                filtered.to_csv(index=False),
                file_name="gap_analysis.csv",
            )

            if not filtered.empty:
                st.subheader("🔗 Inbound Link Details")
                selected_url = st.selectbox(
                    "Select a URL to view who links to it:",
                    options=filtered["URL"].tolist(),
                )
                if selected_url:
                    link_details = anchors_df[
                        anchors_df["ToURL"] == selected_url
                    ][["FromURL", "Anchor Text", "Link Position"]]
                    st.write(f"Inbound links pointing to `{selected_url}`:")
                    st.dataframe(link_details)

    # ──────────────────────────────────────────────────────────────────────────
    # Tab 2: 📊 Funnel Flow Sankey
    # ──────────────────────────────────────────────────────────────────────────
    with tabs[1]:
        # If pages_df or anchors_df is empty, or if there are no funnel transitions, show a warning
        if pages_df.empty or anchors_df.empty:
            st.warning("Not enough data to build a Sankey (check your filters).")
        else:
            # Merge anchors → pages to get From_Funnel
            try:
                merged = anchors_df.merge(
                    pages_df[["URL", "Funnel"]],
                    left_on="FromURL",
                    right_on="URL",
                    how="left",
                ).rename(columns={"Funnel": "From_Funnel"}).drop(columns=["URL"])
            except Exception as e:
                st.error(f"❌ Error merging anchors→pages for From_Funnel: {e}")
                st.stop()

            # Merge that result → pages again for To_Funnel
            try:
                merged = merged.merge(
                    pages_df[["URL", "Funnel"]],
                    left_on="ToURL",
                    right_on="URL",
                    how="left",
                ).rename(columns={"Funnel": "To_Funnel"}).drop(columns=["URL"])
            except Exception as e:
                st.error(f"❌ Error merging anchors→pages for To_Funnel: {e}")
                st.stop()

            # Group by Funnel transitions
            sankey_df = (
                merged.groupby(["From_Funnel", "To_Funnel"])
                .size()
                .reset_index(name="Count")
            )

            # If there are no transitions at all, warn and skip plotting
            if sankey_df.empty:
                st.warning("No funnel‐to‐funnel links available for the current filters.")
            else:
                # Determine which funnel labels actually exist, in the “Top/Mid/Bottom” order
                funnel_order = ["Top", "Mid", "Bottom"]
                existing_labels = sorted(
                    set(funnel_order)
                    & set(sankey_df["From_Funnel"]).union(sankey_df["To_Funnel"])
                )

                # If even after intersection there are no valid labels, warn and skip
                if not existing_labels:
                    st.warning("Filtered funnel labels do not match any known stages (Top/Mid/Bottom).")
                else:
                    label_map = {label: i for i, label in enumerate(existing_labels)}
                    sankey_df = sankey_df[
                        sankey_df["From_Funnel"].isin(label_map)
                        & sankey_df["To_Funnel"].isin(label_map)
                    ]

                    # If sankey_df is empty now, show a warning
                    if sankey_df.empty:
                        st.warning("No valid funnel transitions remain after filtering stages.")
                    else:
                        # Build and display the Sankey
                        with st.spinner("Building Funnel Sankey..."):
                            fig = go.Figure(
                                data=[
                                    go.Sankey(
                                        node=dict(
                                            label=existing_labels,
                                            pad=20,
                                            thickness=20,
                                        ),
                                        link=dict(
                                            source=sankey_df["From_Funnel"].map(label_map),
                                            target=sankey_df["To_Funnel"].map(label_map),
                                            value=sankey_df["Count"],
                                        ),
                                    )
                                ]
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        st.subheader("🔢 Funnel Link Transitions Table")
                        st.dataframe(sankey_df)
                        st.download_button(
                            "📥 Download Sankey Table",
                            sankey_df.to_csv(index=False),
                            file_name="funnel_transitions.csv",
                        )

                        st.subheader("🔎 Explore Specific Funnel Transition")
                        transition_options = sankey_df.apply(
                            lambda row: f"{row['From_Funnel']} → {row['To_Funnel']}", axis=1
                        ).tolist()
                        selected_transition = st.selectbox(
                            "Select a transition", options=transition_options
                        )

                        if selected_transition:
                            from_funnel, to_funnel = selected_transition.split(" → ")
                            transition_rows = merged[
                                (merged["From_Funnel"] == from_funnel)
                                & (merged["To_Funnel"] == to_funnel)
                            ]
                            drill_df = transition_rows[["FromURL", "ToURL", "Anchor Text"]]
                            st.dataframe(drill_df)
                            st.download_button(
                                "📥 Download Transition URLs",
                                drill_df.to_csv(index=False),
                                file_name="funnel_transition_details.csv",
                            )
else:
    st.info("👆 Please upload both files to begin.")
