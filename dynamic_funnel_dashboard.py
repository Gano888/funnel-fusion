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
pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV", type="csv")


#
# If both files are uploaded, read them and register in DuckDB.
#
if pages_file and anchors_file:
    # 1) Read each CSV into a Pandas DataFrame
    try:
        pages_df_raw = pd.read_csv(io.StringIO(pages_file.read().decode("utf-8")))
    except Exception as e:
        st.error(f"❌ Failed to read pages CSV: {e}")
        st.stop()

    try:
        text = anchors_file.read().decode("utf-8", errors="replace")
        anchors_df_raw = pd.read_csv(io.StringIO(text))
    except Exception as e:
        st.error(f"❌ Failed to read anchors CSV: {e}")
        st.stop()

    # 2) Load (or reload) those DataFrames into this session’s DuckDB
    con = load_tables(pages_df_raw, anchors_df_raw)

    # 3) Build filter picklists from DuckDB *once* (we'll reuse them in the form)
    try:
        funnel_tuples = con.execute(
            "SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL"
        ).fetchall()
        funnel_list = sorted([row[0] for row in funnel_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Funnel’ from pages table: {e}")
        st.stop()

    try:
        geo_tuples = con.execute(
            "SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL"
        ).fetchall()
        geo_list = sorted([row[0] for row in geo_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Geo’ from pages table: {e}")
        st.stop()

    try:
        position_tuples = con.execute(
            "SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL"
        ).fetchall()
        position_list = sorted([row[0] for row in position_tuples])
    except Exception as e:
        st.error(f"❌ Error querying ‘Link Position’ from anchors table: {e}")
        st.stop()

    #
    # 4) SIDEBAR FORM: wrap all three filters in a form so that 
    #    the app only re-runs filtering once “Apply Filters” is clicked.
    #
    with st.sidebar.form(key="filter_form"):
        selected_funnels = st.multiselect(
            "Funnel Stage(s)",
            funnel_list,
            default=st.session_state.get("selected_funnels", funnel_list),
        )
        selected_geos = st.multiselect(
            "Geo(s)",
            geo_list,
            default=st.session_state.get("selected_geos", geo_list),
        )
        selected_positions = st.multiselect(
            "Link Position(s)",
            position_list,
            default=st.session_state.get("selected_positions", position_list),
        )

        # A submit button: until clicked, other parts of this app won’t re-run.
        apply = st.form_submit_button(label="Apply Filters")

        # Once “Apply Filters” is clicked, stash these selections in session_state:
        if apply:
            st.session_state["selected_funnels"] = selected_funnels
            st.session_state["selected_geos"] = selected_geos
            st.session_state["selected_positions"] = selected_positions

    # If the user has never clicked “Apply Filters” yet, fall back to all-selected:
    if "selected_funnels" not in st.session_state:
        st.session_state["selected_funnels"] = funnel_list
    if "selected_geos" not in st.session_state:
        st.session_state["selected_geos"] = geo_list
    if "selected_positions" not in st.session_state:
        st.session_state["selected_positions"] = position_list

    # Use the st.session_state values for the actual filtering logic:
    selected_funnels = st.session_state["selected_funnels"]
    selected_geos = st.session_state["selected_geos"]
    selected_positions = st.session_state["selected_positions"]

    #
    # 5) APPLY FILTERS: exactly as before, but driven by session_state
    #
    if not selected_funnels or not selected_geos:
        # If either Funnel or Geo is completely deselected, force empty DataFrames
        pages_df = pd.DataFrame(columns=["URL", "Funnel", "Topic", "Geo"])
        anchors_df = pd.DataFrame(
            columns=["FromURL", "ToURL", "Anchor Text", "Link Position"]
        )
    else:
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

    #
    # 6) MAIN UI TABS (only two: Gap Analysis & Funnel Flow)
    #
    tabs = st.tabs(["🔍 Link Gap Analysis", "📊 Funnel Flow"])

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

            all_labels = list(
                set(sankey_df["From_Funnel"].dropna())
                | set(sankey_df["To_Funnel"].dropna())
            )

            label_set = [f for f in funnel_list if f in all_labels]
            if not label_set:
                label_set = sorted(all_labels)

            label_map = {label: i for i, label in enumerate(label_set)}

            sankey_df = sankey_df[
                sankey_df["From_Funnel"].isin(label_map)
                & sankey_df["To_Funnel"].isin(label_map)
            ]

            fig = go.Figure(
                data=[go.Sankey(
                    node=dict(label=label_set, pad=20, thickness=20),
                    link=dict(
                        source=sankey_df["From_Funnel"].map(label_map),
                        target=sankey_df["To_Funnel"].map(label_map),
                        value=sankey_df["Count"],
                    ),
                )]
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
