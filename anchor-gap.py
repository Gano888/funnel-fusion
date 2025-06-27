import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.graph_objects as go
import requests
import datetime
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.set_page_config(layout="wide")
st.title("Internal Link Analysis")

# ──────────────────────────────────────────────────────────────────────────────
# 1) DuckDB helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_duckdb():
    if "duckdb_conn" not in st.session_state:
        st.session_state.duckdb_conn = duckdb.connect(database=":memory:")
    return st.session_state.duckdb_conn

def load_tables(pages_df: pd.DataFrame, anchors_df: pd.DataFrame):
    con = get_duckdb()
    try:
        con.execute("DROP TABLE IF EXISTS pages;")
        con.execute("DROP TABLE IF EXISTS anchors;")
    except duckdb.Error:
        pass
    con.register("pages_view", pages_df)
    con.execute("CREATE TABLE pages AS SELECT * FROM pages_view")
    con.register("anchors_view", anchors_df)
    con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view")
    return con

def to_sql_str_list(items):
    escaped = ["'" + str(i).replace("'", "''") + "'" for i in items]
    return "(" + ", ".join(escaped) + ")"

# ──────────────────────────────────────────────────────────────────────────────
# 2) Upload your service account JSON + Ahrefs token
# ──────────────────────────────────────────────────────────────────────────────
gsc_json_file = st.sidebar.file_uploader(
    "Upload GSC Service Account JSON", type="json",
    help="Your Search Console service account credentials"
)
ahrefs_token = st.sidebar.text_input(
    "Ahrefs API Token", type="password",
    help="Enter your Ahrefs API token"
)

# ──────────────────────────────────────────────────────────────────────────────
# 3) Upload Classification & Inlinks CSVs
# ──────────────────────────────────────────────────────────────────────────────
pages_file   = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV",   type="csv")

if not pages_file or not anchors_file:
    st.info("👆 Please upload both Classification and Inlinks CSVs to proceed.")
    st.stop()

try:
    pages_df_raw   = pd.read_csv(io.StringIO(pages_file.read().decode("utf-8")))
    anchors_df_raw = pd.read_csv(io.StringIO(anchors_file.read().decode("utf-8")))
except Exception as e:
    st.error(f"❌ Failed to read CSV: {e}")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# 4) Load into DuckDB & build filter lists
# ──────────────────────────────────────────────────────────────────────────────
conn = load_tables(pages_df_raw, anchors_df_raw)

funnel_list   = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL"
).fetchall())
geo_list      = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL"
).fetchall())
position_list = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL"
).fetchall())

# ──────────────────────────────────────────────────────────────────────────────
# 5) Sidebar filter form
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar.form(key="filter_form"):
    selected_funnels   = st.multiselect(
        "Funnel Stage(s)", funnel_list,
        default=st.session_state.get("selected_funnels", funnel_list))
    selected_geos      = st.multiselect(
        "Geo(s)", geo_list,
        default=st.session_state.get("selected_geos", geo_list))
    selected_positions = st.multiselect(
        "Link Position(s)", position_list,
        default=st.session_state.get("selected_positions", position_list))
    apply = st.form_submit_button("Apply Filters")
    if apply:
        st.session_state["selected_funnels"]   = selected_funnels
        st.session_state["selected_geos"]      = selected_geos
        st.session_state["selected_positions"] = selected_positions

# Initialize defaults
for key, default in [
    ("selected_funnels",   funnel_list),
    ("selected_geos",      geo_list),
    ("selected_positions", position_list),
]:
    if key not in st.session_state:
        st.session_state[key] = default

sf = st.session_state["selected_funnels"]
sg = st.session_state["selected_geos"]
sp = st.session_state["selected_positions"]

# ──────────────────────────────────────────────────────────────────────────────
# 6) Apply filters
# ──────────────────────────────────────────────────────────────────────────────
if not sf or not sg:
    pages_df   = pd.DataFrame(columns=["URL","Funnel","Topic","Geo","lat","lon"])
    anchors_df = pd.DataFrame(columns=["FromURL","ToURL","Anchor Text","Link Position"])
else:
    pages_sql = f"""
        SELECT *, LOWER(RTRIM(Address,'/')) AS URL
        FROM pages
        WHERE Funnel IN {to_sql_str_list(sf)}
          AND Geo    IN {to_sql_str_list(sg)}
    """
    try:
        pages_df = conn.execute(pages_sql).fetchdf()
    except Exception as e:
        st.error(f"❌ Error running pages filter SQL: {e}")
        st.stop()

    if not sp:
        anchors_df = pd.DataFrame(
            columns=["FromURL","ToURL","Anchor Text","Link Position"])
    else:
        anchors_sql = f"""
            SELECT *,
                   LOWER(RTRIM(Source,'/'))      AS FromURL,
                   LOWER(RTRIM(Destination,'/')) AS ToURL,
                   Anchor                         AS "Anchor Text"
            FROM anchors
            WHERE "Link Position" IN {to_sql_str_list(sp)}
        """
        try:
            anchors_df = conn.execute(anchors_sql).fetchdf()
        except Exception as e:
            st.error(f"❌ Error running anchors filter SQL: {e}")
            st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# 7) Tabs: Gap Analysis, Funnel Flow, Page Explorer
# ──────────────────────────────────────────────────────────────────────────────
tabs = st.tabs(["🔍 Link Gap Analysis", "📊 Funnel Flow", "🕵️ Page Explorer"])

# --- Tab 0: Link Gap Analysis ---
with tabs[0]:
    if pages_df.empty:
        st.warning("No pages to display (check your filters).")
    else:
        inbound_counts = (
            anchors_df.groupby("ToURL")["Anchor Text"]
            .count().reset_index(name="InboundLinks")
        )
        gap_df = pages_df.merge(
            inbound_counts, left_on="URL", right_on="ToURL", how="left"
        )
        gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)

        max_links = int(gap_df["InboundLinks"].max())
        threshold = st.slider("Maximum Inbound Links", 0, max_links, max_links)
        filtered = gap_df[gap_df["InboundLinks"] <= threshold][
            ["URL","Funnel","Topic","Geo","InboundLinks"]
        ]

        st.dataframe(filtered, use_container_width=True)
        st.download_button(
            "📥 Download Gap Results",
            filtered.to_csv(index=False),
            file_name="gap_analysis.csv",
        )

        if not filtered.empty:
            sel = st.selectbox("Select a URL", filtered["URL"])
            link_details = anchors_df[anchors_df["ToURL"]==sel][
                ["FromURL","Anchor Text","Link Position"]
            ]
            st.subheader(f"Inbound links to {sel}")
            st.dataframe(link_details, use_container_width=True)

# --- Tab 1: Funnel Flow Sankey ---
with tabs[1]:
    if pages_df.empty or anchors_df.empty:
        st.warning("Not enough data for Sankey (check your filters).")
    else:
        merged = (
            anchors_df
            .merge(pages_df[["URL","Funnel"]], left_on="FromURL", right_on="URL", how="left")
            .rename(columns={"Funnel":"From_Funnel"}).drop(columns="URL")
            .merge(pages_df[["URL","Funnel"]], left_on="ToURL", right_on="URL", how="left")
            .rename(columns={"Funnel":"To_Funnel"}).drop(columns="URL")
        )
        sankey_df = (
            merged.groupby(["From_Funnel","To_Funnel"])
            .size().reset_index(name="Count")
        )

        labels = set(sankey_df["From_Funnel"]) | set(sankey_df["To_Funnel"])
        label_set = [f for f in funnel_list if f in labels] or sorted(labels)
        label_map = {l:i for i,l in enumerate(label_set)}

        sankey_df = sankey_df[
            sankey_df["From_Funnel"].isin(label_map)
            & sankey_df["To_Funnel"].isin(label_map)
        ]

        if sankey_df.empty:
            st.warning("No transitions for these filters.")
        else:
            fig = go.Figure(go.Sankey(
                node=dict(label=label_set,pad=20,thickness=20),
                link=dict(
                    source=sankey_df["From_Funnel"].map(label_map),
                    target=sankey_df["To_Funnel"].map(label_map),
                    value=sankey_df["Count"]
                )
            ))
            st.plotly_chart(fig, use_container_width=True)
            st.subheader("🔢 Funnel Link Transitions Table")
            st.dataframe(sankey_df, use_container_width=True)

# --- Tab 2: Page Explorer ---
with tabs[2]:
    st.header("Page Explorer: GSC & Ahrefs")

    # Page selector
    if pages_df.empty:
        st.warning("No pages available.")
        st.stop()
    page = st.selectbox("Select a page", pages_df["URL"].unique())

    # --- GSC API pull ---
    if not gsc_json_file:
        st.info("Upload your GSC service-account JSON to fetch Search Console data.")
    else:
        try:
            sa_info = json.load(gsc_json_file)
            creds = service_account.Credentials.from_service_account_info(
                sa_info,
                scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
            )
            gsc = build("searchconsole", "v1", credentials=creds)

            end = datetime.date.today()
            start = end - datetime.timedelta(days=90)
            body = {
                "startDate": start.isoformat(),
                "endDate":   end.isoformat(),
                "dimensions": ["query"],
                "dimensionFilterGroups": [{
                    "filters": [{
                        "dimension": "page",
                        "operator":  "equals",
                        "expression": page
                    }]
                }],
                "rowLimit": 50
            }
            resp = gsc.searchanalytics().query(
                siteUrl=f"https://{sg[0]}", body=body
            ).execute()
            rows = resp.get("rows", [])
            if not rows:
                st.warning("No GSC data for this page.")
            else:
                gsc_df = pd.DataFrame(rows)
                gsc_df.columns = ["query","impressions","clicks","ctr","position"]
                st.subheader("Top Queries")
                st.dataframe(
                    gsc_df.sort_values("impressions", ascending=False).head(10),
                    use_container_width=True
                )
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Impressions", int(gsc_df["impressions"].sum()))
                c2.metric("Clicks",      int(gsc_df["clicks"].sum()))
                c3.metric("CTR",         f"{gsc_df['ctr'].mean():.1%}")
                c4.metric("Avg Position",f"{gsc_df['position'].mean():.1f}")
        except Exception as e:
            st.error(f"GSC API error: {e}")

    # --- Internal anchors ---
    st.subheader("Internal Anchors")
    intern = anchors_df[anchors_df["ToURL"]==page]
    if intern.empty:
        st.write("No internal links to this page.")
    else:
        st.dataframe(intern[["FromURL","Anchor Text","Link Position"]], use_container_width=True)

    # --- Ahrefs API pull ---
    st.subheader("External Backlinks (Ahrefs)")
    if not ahrefs_token:
        st.info("Enter your Ahrefs API token to fetch external backlinks.")
    else:
        ah_url = (
            f"https://apiv3.ahrefs.com?token={ahrefs_token}"
            f"&target={page}&from=backlinks&limit=100&output=json"
        )
        try:
            data = requests.get(ah_url, timeout=10).json().get("backlinks", [])
            ext = pd.DataFrame(data)
            if ext.empty:
                st.write("No external backlinks found.")
            else:
                st.dataframe(
                    ext[["referring_domain","anchor","backlinks"]],
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Ahrefs API error: {e}")
