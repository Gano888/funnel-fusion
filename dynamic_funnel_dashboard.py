import streamlit as st
import duckdb
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk

nltk.download('punkt', quiet=True)

st.set_page_config(layout="wide")

st.title("Funnel Fusion Dashboard (DuckDB Optimized)")
st.markdown("Upload your classification and inlink files to begin analysis.")

@st.cache_data(show_spinner=False)
def load_duckdb(pages_file, anchors_file):
    con = duckdb.connect(database=':memory:')
    con.execute(f"""
        CREATE TABLE pages AS 
        SELECT *, LOWER(RTRIM(Address, '/')) AS URL
        FROM read_csv_auto('{pages_file.name}', HEADER=TRUE);
    """)
    con.execute(f"""
        CREATE TABLE anchors AS 
        SELECT *, LOWER(RTRIM(Source, '/')) AS From, LOWER(RTRIM(Destination, '/')) AS To
        FROM read_excel('{anchors_file.name}', sheet=0);
    """)
    return con

pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks Excel", type="xlsx")

if pages_file and anchors_file:
    con = load_duckdb(pages_file, anchors_file)

    unique_funnels = con.execute("SELECT DISTINCT Funnel FROM pages WHERE Funnel IS NOT NULL").fetchdf()["Funnel"].tolist()
    selected_funnels = st.sidebar.multiselect("Select Funnel Stage(s):", options=unique_funnels, default=unique_funnels)

    unique_topics = con.execute("SELECT DISTINCT UNNEST(STRING_SPLIT(Topic, ',')) AS Topic FROM pages").fetchdf()["Topic"].dropna().str.strip().unique().tolist()
    selected_topics = st.sidebar.multiselect("Select Topic(s):", options=unique_topics, default=unique_topics)

    unique_geos = con.execute("SELECT DISTINCT Geo FROM pages WHERE Geo IS NOT NULL").fetchdf()["Geo"].tolist()
    selected_geos = st.sidebar.multiselect("Select Geo(s):", options=unique_geos, default=unique_geos)

    link_positions = con.execute("SELECT DISTINCT \"Link Position\" FROM anchors WHERE \"Link Position\" IS NOT NULL").fetchdf()["Link Position"].tolist()
    selected_link_pos = st.sidebar.multiselect("Select Link Position(s):", options=link_positions, default=["Content"])

    query = f"""
        SELECT a.*, 
               pf.Funnel AS From_Funnel, pf.Topic AS From_Topic, pf.Geo AS From_Geo,
               pt.Funnel AS To_Funnel, pt.Topic AS To_Topic, pt.Geo AS To_Geo
        FROM anchors a
        LEFT JOIN pages pf ON a.From = pf.URL
        LEFT JOIN pages pt ON a.To = pt.URL
        WHERE a."Link Position" IN ({','.join(f'\'{lp}\'' for lp in selected_link_pos)})
    """
    df = con.execute(query).fetchdf()

    if selected_funnels:
        df = df[df["From_Funnel"].isin(selected_funnels) & df["To_Funnel"].isin(selected_funnels)]
    if selected_topics:
        df = df[df["From_Topic"].fillna('').apply(lambda x: any(t.strip() in selected_topics for t in x.split(','))) &
              df["To_Topic"].fillna('').apply(lambda x: any(t.strip() in selected_topics for t in x.split(',')))]
    if selected_geos:
        df = df[df["From_Geo"].isin(selected_geos) & df["To_Geo"].isin(selected_geos)]

    tabs = st.tabs(["Gap Analysis", "Network Graph", "Sankey", "Topic Heatmap", "Anchors", "Anchor Usage"])

    with tabs[0]:
        st.header("Link Gap Finder")
        inbound_counts = df.groupby("To")["Anchor Text"].count().reset_index(name="InboundLinks")
        gap_df = df.merge(inbound_counts, on="To", how="left")
        gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)
        threshold = st.slider("Max inbound links", 0, int(gap_df["InboundLinks"].max()), 2)
        filtered_gap = gap_df[gap_df["InboundLinks"] <= threshold][["To", "To_Funnel", "To_Topic", "To_Geo", "InboundLinks"]].drop_duplicates()
        st.dataframe(filtered_gap)
        st.download_button("Download Gap Table", data=filtered_gap.to_csv(index=False), file_name="gap_analysis.csv")

    with tabs[1]:
        st.header("Internal Link Graph")
        G = nx.from_pandas_edgelist(df, source='From', target='To', edge_attr='Anchor Text', create_using=nx.DiGraph())
        pos = nx.kamada_kawai_layout(G)
        fig = go.Figure()
        for src, dst in G.edges():
            x0, y0 = pos[src]
            x1, y1 = pos[dst]
            fig.add_trace(go.Scatter(x=[x0, x1, None], y=[y0, y1, None], mode='lines', line=dict(width=1, color='gray')))
        for node in G.nodes():
            x, y = pos[node]
            fig.add_trace(go.Scatter(x=[x], y=[y], mode='markers+text', text=[node], textposition="bottom center", marker=dict(size=8)))
        st.plotly_chart(fig)

    with tabs[2]:
        st.header("Funnel Flow Sankey")
        sankey_data = df.groupby(["From_Funnel", "To_Funnel"]).size().reset_index(name="Count")
        funnel_stages = ["Top", "Mid", "Bottom"]
        f_map = {k: i for i, k in enumerate(funnel_stages)}
        sankey_data = sankey_data[sankey_data["From_Funnel"].isin(f_map) & sankey_data["To_Funnel"].isin(f_map)]
        fig = go.Figure(data=[go.Sankey(
            node=dict(label=funnel_stages, pad=15, thickness=20),
            link=dict(
                source=sankey_data["From_Funnel"].map(f_map),
                target=sankey_data["To_Funnel"].map(f_map),
                value=sankey_data["Count"]
            ))])
        st.plotly_chart(fig)

    with tabs[3]:
        st.header("Topic Transition Heatmap")
        topic_counts = df.groupby(["From_Topic", "To_Topic"]).size().reset_index(name="Count")
        heatmap = topic_counts.pivot("From_Topic", "To_Topic", "Count").fillna(0)
        fig = px.imshow(heatmap, text_auto=True, labels=dict(x="To", y="From", color="Links"))
        st.plotly_chart(fig)

    with tabs[4]:
        st.header("Anchor Word Cloud & Top Words")
        text = " ".join(df["Anchor Text"].dropna().astype(str))
        wc = WordCloud(width=800, height=400, background_color="white").generate(text)
        fig_wc, ax = plt.subplots(figsize=(8, 4))
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig_wc)
        words = nltk.word_tokenize(text)
        words = [word.lower() for word in words if word.isalpha()]
        counter = Counter(words)
        df_common = pd.DataFrame(counter.most_common(20), columns=["Word", "Frequency"])
        st.bar_chart(df_common.set_index("Word"))

    with tabs[5]:
        st.header("Anchor Usage Frequency")
        usage = df.groupby("Anchor Text").agg(Occurrences=("Anchor Text", "count"), UniquePages=("To", pd.Series.nunique)).reset_index()
        st.dataframe(usage.sort_values("Occurrences", ascending=False).head(20))
        st.download_button("Download Anchor Usage", data=usage.to_csv(index=False), file_name="anchor_usage.csv")

else:
    st.info("Please upload both required files to begin.")
