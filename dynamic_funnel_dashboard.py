import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("Internal Link Analysis")


def get_duckdb():
    """
    Returns a session-specific DuckDB connection (in-memory).
    Stored under st.session_state so each user/tab has its own.
    """
    if "duckdb_conn" not in st.session_state:
        conn = duckdb.connect(database=":memory:")
        st.session_state.duckdb_conn = conn
    return st.session_state.duckdb_conn


def load_tables(pages_df: pd.DataFrame, anchors_df: pd.DataFrame):
    """
    Given two Pandas DataFrames (pages_df, anchors_df), 
    drop old tables (if any) and create new ones in this session's DuckDB.
    """
    con = get_duckdb()

    # Drop existing tables if they were created in a previous upload
    try:
        con.execute("DROP TABLE IF EXISTS pages;")
        con.execute("DROP TABLE IF EXISTS anchors;")
    except duckdb.Error:
        pass

    # Register pages_df as a DuckDB view/table
    con.register("pages_view", pages_df)
    con.execute("CREATE TABLE pages AS SELECT * FROM pages_view")

    # Register anchors_df as a DuckDB view/table
    con.register("anchors_view", anchors_df)
    con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view")

    return con


def to_sql_str_list(items):
    """
    Safely convert a non-empty Python list of strings/numbers
    into a parenthesized, single-quoted SQL list:
      ["US","CA"] -> "('US','CA')"
    Caller must ensure `items` is not empty.
    """
    escaped = ["'" + str(i).replace("'", "''") + "'" for i in items]
    return "(" + ", ".join(escaped) + ")"


# ---------------- Upload Interface ----------------
pages_file   = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV",   type="csv")

if pages_file and anchors_file:
    # 1) Read each CSV into a Pandas DataFrame
    try:
        pages_df_raw = pd.read_csv(io.StringIO(pages_file.read().decode("utf-8")))
    except Exception as e:
        st.error(f"❌ Failed to read pages CSV: {e}")
        st.stop()

    try:
        anchors_df_raw = pd.read_csv(io.StringIO(anchors_file.read().decode("utf-8")))
    except Exception as e:
        st.error(f"❌ Failed to read anchors CSV: {e}")
        st.stop()

    # 2) Load (or reload) those DataFrames into this session’s DuckDB
    con = load_tables(pages_df_raw, anchors_df_raw)

    # 3) Build filter picklists from DuckDB
    try:
        funnel_tuples = con.execute(
            "SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL"
        ).fetchall()
        funnel_list = sorted([row[0] for row in funnel_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Funnel’ from pages table: {e}")
        st.stop()

    selected_funnels = st.sidebar.multiselect(
        "Funnel Stage(s)", funnel_list, default=funnel_list
    )

    try:
        geo_tuples = con.execute(
            "SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL"
        ).fetchall()
        geo_list = sorted([row[0] for row in geo_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Geo’ from pages table: {e}")
        st.stop()

    selected_geos = st.sidebar.multiselect("Geo(s)", geo_list, default=geo_list)

    try:
        position_tuples = con.execute(
            "SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL"
        ).fetchall()
        position_list = sorted([row[0] for row in position_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Link Position’ from anchors table: {e}")
        st.stop()

    selected_positions = st.sidebar.multiselect(
        "Link Position(s)", position_list, default=["Content"]
    )

    # 4) If Funnels or Geos are completely deselected, force empty DataFrames
    if not selected_funnels or not selected_geos:
        pages_df = pd.DataFrame(columns=["URL", "Funnel", "Topic", "Geo", "lat", "lon"])
        anchors_df = pd.DataFrame(
            columns=["FromURL", "ToURL", "Anchor Text", "Link Position"]
        )
    else:
        # Build & run pages_sql safely
        pages_sql = f"""
            SELECT
                *,
                LOWER(RTRIM(Address, '/')) AS URL
            FROM pages
            WHERE Funnel IN {to_sql_str_list(selected_funnels)}
              AND Geo    IN {to_sql_str_list(selected_geos)}
        """
        try:
            pages_df = con.execute(pages_sql).fetchdf()
        except Exception as e:
            st.error(f"❌ Error running pages filter SQL: {e}")
            st.stop()

        # Build anchors_sql only if there is at least one Link-Position
        if not selected_positions:
            anchors_df = pd.DataFrame(
                columns=["FromURL", "ToURL", "Anchor Text", "Link Position"]
            )
        else:
            anchors_sql = f"""
                SELECT
                    *,
                    LOWER(RTRIM(Source, '/'))      AS FromURL,
                    LOWER(RTRIM(Destination, '/')) AS ToURL,
                    Anchor                         AS "Anchor Text"
                FROM anchors
                WHERE "Link Position" IN {to_sql_str_list(selected_positions)}
            """
            try:
                anchors_df = con.execute(anchors_sql).fetchdf()
            except Exception as e:
                st.error(f"❌ Error running anchors filter SQL: {e}")
                st.stop()

    # 5) Main UI Tabs
    tabs = st.tabs(["🔍 Link Gap Analysis", "📊 Funnel Flow", "🗺️ Geo Map"])

    # -------------- Tab 1: Link Gap Analysis --------------
    with tabs[0]:
        if pages_df.empty:
            st.warning("No pages to display (check your Funnel/Geo selections).")
        else:
            inbound_counts = (
                anchors_df.groupby("ToURL")["Anchor Text"]
                .count()
                .reset_index(name="InboundLinks")
            )
            gap_df = pages_df.merge(
                inbound_counts, left_on="URL", right_on="ToURL", how="left"
            )
            gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)

            max_links = int(gap_df["InboundLinks"].max()) if not gap_df.empty else 0
            threshold = st.slider("Maximum Inbound Links", 0, max_links, max_links)

            filtered = gap_df[
                gap_df["InboundLinks"] <= threshold
            ][["URL", "Funnel", "Topic", "Geo", "InboundLinks"]]

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

    # -------------- Tab 2: Funnel Flow Sankey --------------
    with tabs[1]:
        if pages_df.empty or anchors_df.empty:
            st.warning("Not enough data to build a Sankey (check your filters).")
        else:
            merged = anchors_df.merge(
                pages_df[["URL", "Funnel"]],
                left_on="FromURL", right_on="URL", how="left"
            ).rename(columns={"Funnel": "From_Funnel"}).drop(columns=["URL"])

            merged = merged.merge(
                pages_df[["URL", "Funnel"]],
                left_on="ToURL", right_on="URL", how="left"
            ).rename(columns={"Funnel": "To_Funnel"}).drop(columns=["URL"])

            sankey_df = (
                merged.groupby(["From_Funnel", "To_Funnel"])
                .size()
                .reset_index(name="Count")
            )

            funnel_order = ["Top", "Mid", "Bottom"]
            label_set = sorted(
                set(funnel_order)
                & set(sankey_df["From_Funnel"]).union(sankey_df["To_Funnel"])
            )
            label_map = {label: i for i, label in enumerate(label_set)}

            sankey_df = sankey_df[
                sankey_df["From_Funnel"].isin(label_map)
                & sankey_df["To_Funnel"].isin(label_map)
            ]

            fig = go.Figure(
                data=[
                    go.Sankey(
                        node=dict(label=label_set, pad=20, thickness=20),
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

    # -------------- Tab 3: Geo Map --------------
    with tabs[2]:
        # 1) Don’t attempt to draw a map if pages_df is empty:
        if pages_df.empty:
            st.info("No geographic data to show (check your Geo filter).")
        else:
            # 2) Wrap your map code in try/except, in case Geo codes are invalid or lat/lon are missing:
            try:
                # If pages_df already has 'lat' and 'lon' columns:
                subset = pages_df[["lat", "lon"]].dropna()
                if subset.empty:
                    st.warning("Selected Geo(s) contain no latitude/longitude data.")
                else:
                    st.map(subset)
                    # ─ If you’re using Plotly for a choropleth, replace with:
                    # fig = px.choropleth(
                    #     pages_df,
                    #     locations="Geo",
                    #     locationmode="country names",
                    #     color="InboundLinks",
                    #     hover_name="URL"
                    # )
                    # st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Map failed to render: {e}")

else:
    st.info("👆 Please upload both files to begin.")
