import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.graph_objects as go
import requests
import datetime
import json
from urllib.parse import urlparse
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
    # drop & recreate
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
    escaped = ["'{}'".format(str(i).replace("'", "''")) for i in items]
    return "(" + ", ".join(escaped) + ")"

# ──────────────────────────────────────────────────────────────────────────────
# 2) Auth inputs
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
# 3) CSV uploads
# ──────────────────────────────────────────────────────────────────────────────
pages_file   = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks CSV", type="csv")

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
# 4) Load into DuckDB & derive filter options
# ──────────────────────────────────────────────────────────────────────────────
conn = load_tables(pages_df_raw, anchors_df_raw)

funnel_list = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL"
).fetchall())
geo_list = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL"
).fetchall())
pos_list = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL"
).fetchall())

# ──────────────────────────────────────────────────────────────────────────────
# 5) Sidebar filters
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar.form(key="filter_form"):
    sel_funnels = st.multiselect(
        "Funnel Stage(s)", funnel_list,
        default=st.session_state.get("sel_funnels", funnel_list))
    sel_geos    = st.multiselect(
        "Geo(s)", geo_list,
        default=st.session_state.get("sel_geos", geo_list))
    sel_pos     = st.multiselect(
        "Link Position(s)", pos_list,
        default=st.session_state.get("sel_pos", pos_list))
    apply_btn = st.form_submit_button("Apply Filters")
    if apply_btn:
        st.session_state.sel_funnels = sel_funnels
        st.session_state.sel_geos    = sel_geos
        st.session_state.sel_pos     = sel_pos

# initialize defaults if missing
for key, default in [
    ("sel_funnels", funnel_list),
    ("sel_geos",    geo_list),
    ("sel_pos",     pos_list)
]:
    if key not in st.session_state:
        st.session_state[key] = default

sf = st.session_state.sel_funnels
sg = st.session_state.sel_geos
sp = st.session_state.sel_pos

# ──────────────────────────────────────────────────────────────────────────────
# 6) Apply filters via DuckDB
# ──────────────────────────────────────────────────────────────────────────────
def get_filtered_data():
    if not sf or not sg:
        return pd.DataFrame(), pd.DataFrame()
    pages_sql = f"""
        SELECT *, LOWER(RTRIM(Address,'/')) AS URL
        FROM pages
        WHERE Funnel IN {to_sql_str_list(sf)}
          AND Geo    IN {to_sql_str_list(sg)}
    """
    pages_df = conn.execute(pages_sql).fetchdf()

    if sp:
        anchors_sql = f"""
            SELECT *,
                   LOWER(RTRIM(Source,'/'))      AS FromURL,
                   LOWER(RTRIM(Destination,'/')) AS ToURL,
                   Anchor                         AS "Anchor Text"
            FROM anchors
            WHERE "Link Position" IN {to_sql_str_list(sp)}
        """
        anchors_df = conn.execute(anchors_sql).fetchdf()
    else:
        anchors_df = pd.DataFrame(
            columns=["FromURL","ToURL","Anchor Text","Link Position"]
        )

    return pages_df, anchors_df

pages_df, anchors_df = get_filtered_data()

# ──────────────────────────────────────────────────────────────────────────────
# 7) Layout tabs
# ──────────────────────────────────────────────────────────────────────────────
tab_gap, tab_flow, tab_explorer = st.tabs([
    "🔍 Link Gap Analysis",
    "📊 Funnel Flow",
    "🕵️ Page Explorer"
])

# --- Tab 0: Link Gap Analysis ---
with tab_gap:
    if pages_df.empty:
        st.warning("No pages to display (check filters).")
    else:
        inbound = (
            anchors_df
            .groupby("ToURL")["Anchor Text"]
            .count()
            .reset_index(name="InboundLinks")
        )
        gap = pages_df.merge(
            inbound, left_on="URL", right_on="ToURL", how="left"
        ).fillna({"InboundLinks": 0})
        gap["InboundLinks"] = gap["InboundLinks"].astype(int)

        max_links = int(gap["InboundLinks"].max())
        threshold = st.slider(
            "Maximum Inbound Links",
            0, max_links, max_links
        )
        filtered_gap = gap[gap["InboundLinks"] <= threshold][
            ["URL","Funnel","Topic","Geo","InboundLinks"]
        ]

        st.dataframe(filtered_gap, use_container_width=True)
        st.download_button(
            "📥 Download Gap Results",
            filtered_gap.to_csv(index=False),
            file_name="gap_analysis.csv"
        )

        if not filtered_gap.empty:
            choice = st.selectbox("Select a URL", filtered_gap["URL"])
            details = anchors_df[anchors_df["ToURL"] == choice][
                ["FromURL","Anchor Text","Link Position"]
            ]
            st.subheader(f"Inbound links to {choice}")
            st.dataframe(details, use_container_width=True)

# --- Tab 1: Funnel Flow Sankey ---
with tab_flow:
    if pages_df.empty or anchors_df.empty:
        st.warning("Not enough data for Sankey (check filters).")
    else:
        merged = (
            anchors_df
            .merge(pages_df[["URL","Funnel"]],
                   left_on="FromURL", right_on="URL", how="left")
            .rename(columns={"Funnel": "From_Funnel"})
            .drop(columns="URL")
            .merge(pages_df[["URL","Funnel"]],
                   left_on="ToURL", right_on="URL", how="left")
            .rename(columns={"Funnel": "To_Funnel"})
            .drop(columns="URL")
        )
        sankey_df = (
            merged.groupby(["From_Funnel","To_Funnel"])
            .size()
            .reset_index(name="Count")
        )

        labels = sorted(set(sankey_df["From_Funnel"]) |
                        set(sankey_df["To_Funnel"]))
        label_map = {lbl: idx for idx, lbl in enumerate(labels)}

        # drop any rows with null funnels
        sankey_df = sankey_df.dropna(subset=["From_Funnel","To_Funnel"])

        if sankey_df.empty:
            st.warning("No funnel transitions for these filters.")
        else:
            fig = go.Figure(go.Sankey(
                node=dict(label=labels, pad=20, thickness=20),
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
with tab_explorer:
    st.header("Page Explorer: GSC & Ahrefs")

    # Page selector
    if pages_df.empty:
        st.warning("No pages available.")
        st.stop()
    page = st.selectbox("Select a page", pages_df["URL"].unique())

    # ---- GSC Pull with Country Filter ----
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

            # Date range
            end   = datetime.date.today()
            start = end - datetime.timedelta(days=90)

            body = {
                "startDate": start.isoformat(),
                "endDate":   end.isoformat(),
                "dimensions": ["query", "country"],
                "dimensionFilterGroups": [{
                    "filters": [{
                        "dimension":  "page",
                        "operator":   "equals",
                        "expression": page
                    }]
                }],
                "rowLimit": 5000
            }

            site_url = f"{urlparse(page).scheme}://{urlparse(page).netloc}"
            resp = gsc.searchanalytics().query(
                siteUrl=site_url,
                body=body
            ).execute()

            rows = resp.get("rows", [])
            if not rows:
                st.warning("No GSC data for this page.")
            else:
                # Rebuild DataFrame manually to avoid column mismatch
                records = []
                for r in rows:
                    keys = r.get("keys", [])
                    records.append({
                        "query":       keys[0] if len(keys) > 0 else None,
                        "country":     keys[1] if len(keys) > 1 else None,
                        "impressions": r.get("impressions", 0),
                        "clicks":      r.get("clicks", 0),
                        "ctr":         r.get("ctr", 0),
                        "position":    r.get("position", 0)
                    })
                gsc_df = pd.DataFrame(records)

                # Country filter UI
                countries = sorted(gsc_df["country"].dropna().unique())
                sel_ctr   = st.multiselect(
                    "Filter queries by country", countries, default=countries
                )
                filt_df = gsc_df[gsc_df["country"].isin(sel_ctr)]

                st.subheader("Top Queries")
                st.dataframe(
                    filt_df.sort_values("impressions", ascending=False).head(10),
                    use_container_width=True
                )

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Impressions",  int(filt_df["impressions"].sum()))
                c2.metric("Clicks",       int(filt_df["clicks"].sum()))
                c3.metric("CTR",          f"{filt_df['ctr'].mean():.1%}")
                c4.metric("Avg Position", f"{filt_df['position'].mean():.1f}")

        except Exception as e:
            st.error(f"GSC API error: {e}")

    # ---- Internal Anchors ----
    st.subheader("Internal Anchors")
    intern = anchors_df[anchors_df["ToURL"] == page]
    if intern.empty:
        st.write("No internal links to this page.")
    else:
        st.dataframe(
            intern[["FromURL", "Anchor Text", "Link Position"]],
            use_container_width=True
        )

    # ---- Ahrefs Pull ----
    st.subheader("External Backlinks (Ahrefs)")
    if not ahrefs_token:
        st.info("Enter your Ahrefs API token to fetch external backlinks.")
    else:
        params = {
            "token":  ahrefs_token,
            "target": page,
            "from":   "backlinks",
            "limit":  100,
            "output": "json"
        }
        try:
            resp = requests.get(
                "https://apiv3.ahrefs.com",
                params=params,
                timeout=10
            )
            resp.raise_for_status()
            payload = resp.json()
            data    = payload.get("backlinks", [])
            if not data:
                st.write("No external backlinks found.")
            else:
                ext_df = pd.DataFrame(data)
                st.dataframe(
                    ext_df[["referring_domain", "anchor", "backlinks"]],
                    use_container_width=True
                )
        except requests.exceptions.HTTPError as e:
            st.error(f"Ahrefs HTTP error: {e.response.status_code} – {e.response.text}")
        except ValueError as e:
            st.error(f"Ahrefs API error: invalid JSON ({e})")
        except Exception as e:
            st.error(f"Ahrefs API error: {e}")
