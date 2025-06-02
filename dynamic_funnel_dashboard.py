import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("Internal Link Analysis")

@st.cache_resource(show_spinner=False)
def load_duckdb(pages_file, anchors_file):
    con = duckdb.connect(database=":memory:")

    pages_buffer = io.StringIO(pages_file.read().decode("utf-8"))
    con.register("pages_view", pd.read_csv(pages_buffer))
    con.execute("CREATE TABLE pages AS SELECT * FROM pages_view")

    anchors_buffer = io.StringIO(anchors_file.read().decode("utf-8"))
    con.register("anchors_view", pd.read_csv(anchors_buffer))
    con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view")

    return con

def to_sql_str_list(items):
    escaped = ["'" + str(i).replace("'", "''") + "'" for i in items]
    return "(" + ", ".join(escaped) + ")"

# ---------------- Upload Interface ----------------
pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV", type="csv")

if pages_file and anchors_file:
    con = load_duckdb(pages_file, anchors_file)

    funnel_list = [row[0] for row in con.execute("SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL").fetchall()]
    funnel_list = sorted(funnel_list)
    selected_funnels = st.sidebar.multiselect("Funnel Stage(s)", funnel_list, default=funnel_list)

    geo_list = [row[0] for row in con.execute("SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL").fetchall()]
    geo_list = sorted(geo_list)
    selected_geos = st.sidebar.multiselect("Geo(s)", geo_list, default=geo_list)

    position_list = [row[0] for row in con.execute("SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL").fetchall()]
    position_list = sorted(position_list)
    selected_positions = st.sidebar.multiselect("Link Position(s)", position_list, default=["Content"])

    pages_sql = f"""
        SELECT *, LOWER(RTRIM(Address, '/')) AS URL
        FROM pages
        WHERE Funnel IN {to_sql_str_list(selected_funnels)}
        AND Geo IN {to_sql_str_list(selected_geos)}
    """

    anchors_sql = f"""
        SELECT *, 
               LOWER(RTRIM(Source, '/')) AS FromURL, 
               LOWER(RTRIM(Destination, '/')) AS ToURL,
               Anchor AS "Anchor Text"
        FROM anchors
        WHERE "Link Position" IN {to_sql_str_list(selected_positions)}
    """

    pages_df = con.execute(pages_sql).fetchdf()
    anchors_df = con.execute(anchors_sql).fetchdf()

    tabs = st.tabs(["🔍 Link Gap Analysis", "📊 Funnel Flow"])

    # ---------------- Tab 1: Link Gap Analysis ----------------
    with tabs[0]:
        inbound_counts = anchors_df.groupby("ToURL")["Anchor Text"].count().reset_index(name="InboundLinks")
        gap_df = pages_df.merge(inbound_counts, left_on="URL", right_on="ToURL", how="left")
        gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)

        max_links = int(gap_df["InboundLinks"].max()) if not gap_df.empty else 0
        threshold = st.slider("Maximum Inbound Links", 0, max_links, max_links)

        filtered = gap_df[gap_df["InboundLinks"] <= threshold][["URL", "Funnel", "Topic", "Geo", "InboundLinks"]]
        st.dataframe(filtered)

        st.download_button("📥 Download Gap Results", filtered.to_csv(index=False), file_name="gap_analysis.csv")

        if not filtered.empty:
            st.subheader("🔗 Inbound Link Details")
            selected_url = st.selectbox("Select a URL to view who links to it:", options=filtered["URL"].tolist())
            if selected_url:
                link_details = anchors_df[anchors_df["ToURL"] == selected_url][["FromURL", "Anchor Text", "Link Position"]]
                st.write(f"Inbound links pointing to `{selected_url}`:")
                st.dataframe(link_details)

    # ---------------- Tab 2: Funnel Flow Sankey ----------------
    with tabs[1]:
        merged = anchors_df.merge(
            pages_df[["URL", "Funnel"]],
            left_on="FromURL",
            right_on="URL",
            how="left"
        ).rename(columns={"Funnel": "From_Funnel"}).drop(columns=["URL"])

        merged = merged.merge(
            pages_df[["URL", "Funnel"]],
            left_on="ToURL",
            right_on="URL",
            how="left"
        ).rename(columns={"Funnel": "To_Funnel"}).drop(columns=["URL"])

        sankey_df = merged.groupby(["From_Funnel", "To_Funnel"]).size().reset_index(name="Count")

        funnel_order = ["Top", "Mid", "Bottom"]
        label_set = sorted(set(funnel_order) & set(sankey_df["From_Funnel"]).union(sankey_df["To_Funnel"]))
        label_map = {label: i for i, label in enumerate(label_set)}

        sankey_df = sankey_df[sankey_df["From_Funnel"].isin(label_map) & sankey_df["To_Funnel"].isin(label_map)]

        fig = go.Figure(data=[go.Sankey(
            node=dict(label=label_set, pad=20, thickness=20),
            link=dict(
                source=sankey_df["From_Funnel"].map(label_map),
                target=sankey_df["To_Funnel"].map(label_map),
                value=sankey_df["Count"]
            )
        )])
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🔢 Funnel Link Transitions Table")
        st.dataframe(sankey_df)
        st.download_button("📥 Download Sankey Table", sankey_df.to_csv(index=False), file_name="funnel_transitions.csv")

        # Drill-down table
        st.subheader("🔎 Explore Specific Funnel Transition")
        transition_options = sankey_df.apply(lambda row: f"{row['From_Funnel']} → {row['To_Funnel']}", axis=1).tolist()
        selected_transition = st.selectbox("Select a transition", options=transition_options)

        if selected_transition:
            from_funnel, to_funnel = selected_transition.split(" → ")
            transition_rows = merged[(merged["From_Funnel"] == from_funnel) & (merged["To_Funnel"] == to_funnel)]
            drill_df = transition_rows[["FromURL", "ToURL", "Anchor Text"]]
            st.dataframe(drill_df)
            st.download_button("📥 Download Transition URLs", drill_df.to_csv(index=False), file_name="funnel_transition_details.csv")

else:
    st.info("👆 Please upload both files to begin.")
