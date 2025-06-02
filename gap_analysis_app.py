import streamlit as st
import pandas as pd
import duckdb
import io

st.set_page_config(layout="wide")
st.title("Internal Link Gap Finder (Minimal Version)")

# ------------- Load Files -------------
@st.cache_resource(show_spinner=False)
def load_duckdb(pages_file, anchors_file):
    con = duckdb.connect(database=":memory:")

    pages_buffer = io.StringIO(pages_file.read().decode("utf-8"))
    con.register("pages_view", pd.read_csv(pages_buffer))
    con.execute("CREATE TABLE pages AS SELECT * FROM pages_view")

    anchors_buffer = io.BytesIO(anchors_file.read())
    con.register("anchors_view", pd.read_excel(anchors_buffer, engine="openpyxl"))
    con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view")

    return con

# ------------- Helpers -------------
def quote_list(items):
    return "(" + ", ".join("'" + str(i).replace("'", "''") + "'" for i in items) + ")"


# ------------- Upload Files -------------
pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks Excel", type="xlsx")

if pages_file and anchors_file:
    con = load_duckdb(pages_file, anchors_file)

    # ------------- Sidebar Filters -------------
    funnel_list = con.execute("SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL").fetchall()
    funnel_list = sorted([f[0] for f in funnel_list])
    selected_funnels = st.sidebar.multiselect("Funnel Stage(s)", funnel_list, default=funnel_list)

    geo_list = con.execute("SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL").fetchall()
    geo_list = sorted([g[0] for g in geo_list])
    selected_geos = st.sidebar.multiselect("Geo(s)", geo_list, default=geo_list)

    position_list = con.execute("SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL").fetchall()
    position_list = sorted([p[0] for p in position_list])
    selected_positions = st.sidebar.multiselect("Link Position(s)", position_list, default=["Content"])

    # ------------- Build SQL Queries (Safe Formatting) -------------
    funnels_str = quote_list(selected_funnels)
    geos_str = quote_list(selected_geos)
    positions_str = quote_list(selected_positions)

    pages_sql = f"""
        SELECT *, LOWER(RTRIM(Address, '/')) AS URL
        FROM pages
        WHERE Funnel IN {funnels_str}
        AND Geo IN {geos_str}
    """

    anchors_sql = f"""
        SELECT *, LOWER(RTRIM("From", '/')) AS FromURL, LOWER(RTRIM("To", '/')) AS ToURL
        FROM anchors
        WHERE "Link Position" IN {positions_str}
    """

    pages_df = con.execute(pages_sql).fetchdf()
    anchors_df = con.execute(anchors_sql).fetchdf()

    # ------------- Inbound Link Count & Gap Analysis -------------
    inbounds = anchors_df.groupby("ToURL")["Anchor Text"].count().reset_index(name="InboundLinks")
    gap_df = pages_df.merge(inbounds, left_on="URL", right_on="ToURL", how="left")
    gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)

    st.header("Gap Analysis Table")
    threshold = st.slider("Max inbound links", 0, int(gap_df["InboundLinks"].max()), 2)
    filtered = gap_df[gap_df["InboundLinks"] <= threshold][["URL", "Funnel", "Topic", "Geo", "InboundLinks"]]
    st.dataframe(filtered)

    st.download_button("Download Gap Results", filtered.to_csv(index=False), file_name="gap_analysis.csv")

else:
    st.info("Please upload both files to start.")
