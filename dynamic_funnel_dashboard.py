import streamlit as st
import pandas as pd
import duckdb
import io
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk

nltk.download("punkt", quiet=True)
st.set_page_config(layout="wide")
st.title("Funnel Fusion Dashboard (DuckDB Optimized)")

# ----------------------------
# Load DuckDB in-memory
# ----------------------------
@st.cache_resource(show_spinner=False)
def load_duckdb(pages_file, anchors_file):
    import duckdb
    import io

    con = duckdb.connect(database=":memory:")

    # Load pages CSV
    pages_buffer = io.StringIO(pages_file.read().decode("utf-8"))
    con.register("pages_view", pd.read_csv(pages_buffer))
    con.execute("CREATE TABLE pages AS SELECT * FROM pages_view")

    # Load anchors XLSX
    anchors_buffer = io.BytesIO(anchors_file.read())
    con.register("anchors_view", pd.read_excel(anchors_buffer, engine="openpyxl"))
    con.execute("CREATE TABLE anchors AS SELECT * FROM anchors_view")

    return con


# ----------------------------
# User Upload
# ----------------------------
pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks Excel", type="xlsx")
def to_sql_str_list(py_list):
    return "(" + ", ".join(f"'{s.replace("'", "''")}'" for s in py_list) + ")"

if pages_file and anchors_file:
    con = load_duckdb(pages_file, anchors_file)

    # Load lists for filters
    funnel_list = con.execute("SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL").fetchall()
    funnel_list = sorted([f[0] for f in funnel_list])
    selected_funnels = st.sidebar.multiselect("Funnel Stage(s)", funnel_list, default=funnel_list)

    topic_list = con.execute("SELECT DISTINCT UNNEST(STRING_SPLIT(Topic, ',')) FROM pages").fetchall()
    topic_list = sorted(set(t.strip() for t in [t[0] for t in topic_list if t[0]]))
    selected_topics = st.sidebar.multiselect("Topic(s)", topic_list, default=topic_list)

    geo_list = con.execute("SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL").fetchall()
    geo_list = sorted([g[0] for g in geo_list])
    selected_geos = st.sidebar.multiselect("Geo(s)", geo_list, default=geo_list)

    position_list = con.execute("SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL").fetchall()
    position_list = sorted([p[0] for p in position_list])
    selected_positions = st.sidebar.multiselect("Link Position(s)", position_list, default=["Content"])

    topics_sql = to_sql_str_list(selected_topics)
    funnels_sql = to_sql_str_list(selected_funnels)
    geos_sql = to_sql_str_list(selected_geos)

    # ----------------------------
    # Filtered Pages and Anchors
    # ----------------------------
    pages_query = f"""
        SELECT *, LOWER(RTRIM(Address, '/')) AS URL
        FROM pages
        WHERE Funnel IN {funnels_sql}
        AND Geo IN {geos_sql}
        AND EXISTS (
            SELECT 1
            FROM UNNEST(STRING_SPLIT(Topic, ',')) AS topic
            WHERE topic IN {topics_sql}
        )
    """

    anchors_query = f"""
        SELECT *, LOWER(RTRIM(From, '/')) AS FromURL, LOWER(RTRIM(To, '/')) AS ToURL
        FROM anchors
        WHERE \"Link Position\" IN {tuple(selected_positions)}
    """

    filtered_pages = con.execute(pages_query).fetchdf()
    filtered_anchors = con.execute(anchors_query).fetchdf()

    # Merge pages into anchors
    merged = filtered_anchors.merge(filtered_pages[["URL", "Funnel", "Topic"]], left_on="FromURL", right_on="URL", how="left")\
                             .rename(columns={"Funnel": "From_Funnel", "Topic": "From_Topic"}).drop(columns=["URL"])
    merged = merged.merge(filtered_pages[["URL", "Funnel", "Topic"]], left_on="ToURL", right_on="URL", how="left")\
                   .rename(columns={"Funnel": "To_Funnel", "Topic": "To_Topic"}).drop(columns=["URL"])

    tabs = st.tabs(["Gap Analysis", "Internal Graph", "Sankey", "Topic Heatmap", "Anchors", "Anchor Usage"])

    with tabs[0]:
        st.header("Link Gap Analysis")
        inbounds = merged.groupby("ToURL")["Anchor Text"].count().reset_index(name="InboundLinks")
        gap_df = filtered_pages.merge(inbounds, left_on="URL", right_on="ToURL", how="left")
        gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)
        threshold = st.slider("Max Inbound Links", 0, int(gap_df["InboundLinks"].max()), 2)
        filtered = gap_df[gap_df["InboundLinks"] <= threshold][["URL", "Funnel", "Topic", "Geo", "InboundLinks"]]
        st.dataframe(filtered)
        st.download_button("Download Gap Data", filtered.to_csv(index=False), file_name="gap_analysis.csv")

    with tabs[1]:
        st.header("Internal Link Graph")
        G = nx.from_pandas_edgelist(merged, source="FromURL", target="ToURL", edge_attr="Anchor Text", create_using=nx.DiGraph())
        pos = nx.kamada_kawai_layout(G, weight=None)
        fig = go.Figure()
        for src, dst in G.edges():
            x0, y0 = pos[src]
            x1, y1 = pos[dst]
            fig.add_trace(go.Scatter(x=[x0, x1, None], y=[y0, y1, None], mode="lines", line=dict(width=1, color="gray")))
        for node in G.nodes():
            x, y = pos[node]
            fig.add_trace(go.Scatter(x=[x], y=[y], mode="markers+text", text=[node], textposition="bottom center", marker=dict(size=8)))
        st.plotly_chart(fig)

        selected_url = st.selectbox("Inspect Outbound Links for URL", list(G.nodes))
        if selected_url:
            out_df = merged[merged["FromURL"] == selected_url][["FromURL", "ToURL", "Anchor Text", "To_Funnel", "To_Topic"]]
            st.dataframe(out_df)
            st.download_button("Download Outbound Links", out_df.to_csv(index=False), file_name="outbound_links.csv")

    with tabs[2]:
        st.header("Funnel Flow (Sankey)")
        sankey_df = merged.groupby(["From_Funnel", "To_Funnel"]).size().reset_index(name="Count")
        stages = ["Top", "Mid", "Bottom"]
        stage_map = {s: i for i, s in enumerate(stages)}
        sankey_df = sankey_df[sankey_df["From_Funnel"].isin(stage_map) & sankey_df["To_Funnel"].isin(stage_map)]
        fig = go.Figure(data=[go.Sankey(
            node=dict(label=stages),
            link=dict(
                source=sankey_df["From_Funnel"].map(stage_map),
                target=sankey_df["To_Funnel"].map(stage_map),
                value=sankey_df["Count"]
            )
        )])
        st.plotly_chart(fig)

    with tabs[3]:
        st.header("Topic Heatmap")
        topic_df = merged.groupby(["From_Topic", "To_Topic"]).size().reset_index(name="Count")
        pivot = topic_df.pivot(index="From_Topic", columns="To_Topic", values="Count").fillna(0)
        fig = px.imshow(pivot, text_auto=True)
        st.plotly_chart(fig)

    with tabs[4]:
        st.header("Anchor Text Overview")
        text = " ".join(merged["Anchor Text"].dropna())
        wc = WordCloud(width=800, height=400).generate(text)
        fig_wc, ax = plt.subplots(figsize=(8, 4))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig_wc)

        words = nltk.word_tokenize(text)
        words = [w.lower() for w in words if w.isalpha()]
        freq_df = pd.DataFrame(Counter(words).most_common(20), columns=["Word", "Frequency"])
        st.bar_chart(freq_df.set_index("Word"))

    with tabs[5]:
        st.header("Anchor Usage Table")
        usage = merged.groupby("Anchor Text").agg(Count=("Anchor Text", "count"), UniquePages=("ToURL", "nunique")).reset_index()
        st.dataframe(usage.sort_values("Count", ascending=False).head(30))
        st.download_button("Download Anchor Usage", usage.to_csv(index=False), file_name="anchor_usage.csv")

else:
    st.info("Please upload both files to begin.")
