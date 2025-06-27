import streamlit as st
import pandas as pd
import duckdb
import io
import requests
import datetime
import json
from urllib.parse import urlparse
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.set_page_config(layout="wide")
st.title("Page Explorer: GSC & Ahrefs")

# ──────────────────────────────────────────────────────────────────────────────
# 1) DuckDB helpers & data load
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


def to_sql_list(items):
    esc = [f"'{str(i).replace("'","''")}'" for i in items]
    return f"({','.join(esc)})"

# ──────────────────────────────────────────────────────────────────────────────
# 2) Inputs: Auth + CSV uploads
# ──────────────────────────────────────────────────────────────────────────────
gsc_json_file = st.sidebar.file_uploader("GSC Service Account JSON", type="json")
ahrefs_token  = st.sidebar.text_input("Ahrefs API Token", type="password")
pages_file    = st.sidebar.file_uploader("Pages CSV", type="csv")
anchors_file  = st.sidebar.file_uploader("Inlinks CSV", type="csv")

if not pages_file or not anchors_file:
    st.info("Upload both Pages and Inlinks CSVs to proceed.")
    st.stop()

try:
    pages_raw  = pd.read_csv(io.StringIO(pages_file.read().decode("utf-8")))
    anchors_raw = pd.read_csv(io.StringIO(anchors_file.read().decode("utf-8")))
except Exception as e:
    st.error(f"Failed reading CSV: {e}")
    st.stop()

conn = load_tables(pages_raw, anchors_raw)

# derive filters
funnels = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL"
).fetchall())
geos    = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL"
).fetchall())
pos    = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL"
).fetchall())

# ──────────────────────────────────────────────────────────────────────────────
# 3) Sidebar Filters
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar.form("filters"):
    sel_f = st.multiselect("Funnel(s)", funnels, default=funnels)
    sel_g = st.multiselect("Geo(s)",    geos, default=geos)
    sel_p = st.multiselect("Position(s)", pos, default=pos)
    if st.form_submit_button("Apply Filters"):
        st.session_state.sel_f = sel_f
        st.session_state.sel_g = sel_g
        st.session_state.sel_p = sel_p

# defaults
if "sel_f" not in st.session_state: st.session_state.sel_f = funnels
if "sel_g" not in st.session_state: st.session_state.sel_g = geos
if "sel_p" not in st.session_state: st.session_state.sel_p = pos

sf, sg, sp = st.session_state.sel_f, st.session_state.sel_g, st.session_state.sel_p

# apply filters
def get_filtered():
    if not sf or not sg:
        return pd.DataFrame(), pd.DataFrame()
    pages_sql = (
        f"SELECT *, LOWER(RTRIM(Address,'/')) AS URL "
        f"FROM pages WHERE Funnel IN {to_sql_list(sf)} "
        f"AND Geo IN {to_sql_list(sg)}"
    )
    pages_df = conn.execute(pages_sql).fetchdf()
    if sp:
        anchors_sql = (
            f"SELECT *, LOWER(RTRIM(Source,'/')) AS FromURL, "
            f"LOWER(RTRIM(Destination,'/')) AS ToURL, Anchor AS AnchorText "
            f"FROM anchors WHERE \"Link Position\" IN {to_sql_list(sp)}"
        )
        anchors_df = conn.execute(anchors_sql).fetchdf()
    else:
        anchors_df = pd.DataFrame()
    return pages_df, anchors_df

pages_df, anchors_df = get_filtered()

# ──────────────────────────────────────────────────────────────────────────────
# 4) Page Explorer UI
# ──────────────────────────────────────────────────────────────────────────────
if pages_df.empty:
    st.warning("No pages available after filtering.")
    st.stop()

page = st.selectbox("Select a page", pages_df["URL"].unique())

# ---- GSC Data ----
if not gsc_json_file:
    st.info("Upload GSC JSON to fetch Search Console data.")
else:
    try:
        info = json.load(gsc_json_file)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        gsc = build("searchconsole","v1",credentials=creds)
        end = datetime.date.today()
        start = end - datetime.timedelta(days=90)
        body = {
            "startDate": start.isoformat(),
            "endDate":   end.isoformat(),
            "dimensions": ["query","country"],
            "dimensionFilterGroups": [{
                "filters": [{"dimension":"page","operator":"equals","expression":page}]
            }],
            "rowLimit": 5000
        }
        site_url = f"{urlparse(page).scheme}://{urlparse(page).netloc}"
        rows = gsc.searchanalytics().query(
            siteUrl=site_url, body=body
        ).execute().get("rows", [])
        if not rows:
            st.warning("No GSC data.")
        else:
            recs = []
            for r in rows:
                k = r.get("keys", [])
                recs.append({
                    "query":       k[0] if len(k)>0 else None,
                    "country":     k[1] if len(k)>1 else None,
                    "impressions": r.get("impressions", 0),
                    "clicks":      r.get("clicks", 0),
                    "ctr":         r.get("ctr", 0),
                    "position":    r.get("position", 0)
                })
            gdf = pd.DataFrame(recs)
            ctys = sorted(gdf["country"].dropna().unique())
            sel_ctry = st.multiselect("Filter by country", ctys, default=ctys)
            fg = gdf[gdf["country"].isin(sel_ctry)]
            st.subheader("Top Queries")
            st.dataframe(
                fg.sort_values("impressions",ascending=False).head(10),
                use_container_width=True
            )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Impressions",  int(fg["impressions"].sum()))
            c2.metric("Clicks",       int(fg["clicks"].sum()))
            c3.metric("CTR",          f"{fg['ctr'].mean():.1%}")
            c4.metric("Avg Position", f"{fg['position'].mean():.1f}")
    except Exception as e:
        st.error(f"GSC error: {e}")

# ---- Internal Anchors ----
st.subheader("Internal Anchors")
ai = anchors_df[anchors_df["ToURL"]==page]
if ai.empty:
    st.write("No internal links.")
else:
    st.dataframe(ai[["FromURL","AnchorText","Link Position"]], use_container_width=True)

# ---- Ahrefs Backlinks ----
st.subheader("External Backlinks (Ahrefs)")
if not ahrefs_token:
    st.info("Enter Ahrefs token to fetch backlinks.")
else:
    # Use Ahrefs API v2 endpoint for backlinks and metrics
    base_url = "https://apiv2.ahrefs.com"
    # 1) Fetch backlinks for the exact page
    params = {
        "token":  ahrefs_token,
        "target": page,
        "from":   "backlinks",
        "limit":  100,
        "output": "json",
        "mode":   "exact"
    }
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("backlinks", [])
        # If no page-level results, try prefix mode
        if not data:
            st.info(f"No exact-match backlinks for {page}; trying prefix mode...")
            params["mode"] = "prefix"
            resp = requests.get(base_url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json().get("backlinks", [])
        if not data:
            st.write("No external backlinks found for this page.")
        else:
            ext_df = pd.DataFrame(data)
            # Collect DR per referring domain via metrics endpoint
            domains = ext_df["referring_domain"].unique()
            dr_map = {}
            for d in domains:
                try:
                    m_resp = requests.get(
                        base_url,
                        params={
                            "token":  ahrefs_token,
                            "target": d,
                            "from":   "metrics",
                            "mode":   "domain",
                            "output": "json"
                        }, timeout=10
                    )
                    m_resp.raise_for_status()
                    mr = m_resp.json().get("metrics", [{}])[0]
                    dr_map[d] = mr.get("domain_rating")
                except:
                    dr_map[d] = None
            ext_df["Domain Rating"] = ext_df["referring_domain"].map(dr_map)
            # Determine referring page column
            url_col = None
            if "url_from" in ext_df.columns:
                url_col = "url_from"
            elif "referring_page_url" in ext_df.columns:
                url_col = "referring_page_url"
            # Display
            display_cols = []
            if url_col:
                display_cols.append(url_col)
            display_cols += ["referring_domain", "anchor", "backlinks", "Domain Rating"]
            st.dataframe(ext_df[display_cols], use_container_width=True)
    except requests.exceptions.HTTPError as e:
        st.error(f"Ahrefs HTTP error: {e.response.status_code} – {e.response.text}")
    except ValueError as e:
        st.error(f"Ahrefs JSON error: {e}")
    except Exception as e:
        st.error(f"Ahrefs error: {e}")
