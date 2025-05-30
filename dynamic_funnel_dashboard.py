import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from collections import defaultdict, Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk

nltk.download('punkt', quiet=True)

st.set_page_config(layout="wide")

st.title("Funnel Fusion Dashboard (Dynamic Upload Version)")

st.markdown("Upload your classification and inlink files to begin analysis.")

# --------------------------
# FILE UPLOAD
# --------------------------
pages_file = st.sidebar.file_uploader("Upload Classification CSV", type="csv")
anchors_file = st.sidebar.file_uploader("Upload Inlinks Excel", type="xlsx")

if pages_file and anchors_file:
    # Load classification data
    pages_df = pd.read_csv(pages_file)
    pages_df["URL"] = pages_df["Address"].str.rstrip("/").str.lower()
    pages_df["Topic"] = pages_df["Topic"].fillna("No Topic")

    # Load anchors data from Excel and normalize
    anchors_df = pd.read_excel(anchors_file)
    anchors_df = anchors_df.rename(columns={
        "Source": "From",
        "Destination": "To",
        "Anchor": "Anchor Text"
    })
    anchors_df["From"] = anchors_df["From"].str.rstrip("/").str.lower()
    anchors_df["To"] = anchors_df["To"].str.rstrip("/").str.lower()

    # --------------------------
    # GLOBAL FILTERS
    # --------------------------
    st.sidebar.header("Global Filters")

    # Funnel filter
    unique_funnels = sorted(pages_df["Funnel"].dropna().unique())
    selected_funnels = st.sidebar.multiselect("Select Funnel Stage(s):", options=unique_funnels, default=unique_funnels)

    # Topic filter
    topic_set = set()
    for topics in pages_df["Topic"].dropna():
        for t in str(topics).split(","):
            if t.strip():
                topic_set.add(t.strip())
    unique_topics = sorted(topic_set)
    selected_topics = st.sidebar.multiselect("Select Topic(s):", options=unique_topics, default=unique_topics)

    # Geo filter
    unique_geos = sorted(pages_df["Geo"].dropna().unique())
    selected_geos = st.sidebar.multiselect("Select Geo(s):", options=unique_geos, default=unique_geos)

    # Link Position filter
    link_positions = anchors_df["Link Position"].dropna().unique().tolist()
    selected_link_pos = st.sidebar.multiselect("Select Link Position(s):", options=link_positions, default=["Content"])

    # Apply filters to data
    filtered_pages = pages_df.copy()
    if selected_funnels:
        filtered_pages = filtered_pages[filtered_pages["Funnel"].isin(selected_funnels)]
    if selected_topics:
        filtered_pages = filtered_pages[filtered_pages["Topic"].apply(lambda x: bool(set(str(x).split(",")) & set(selected_topics)))]
    if selected_geos:
        filtered_pages = filtered_pages[filtered_pages["Geo"].isin(selected_geos)]

    anchors_df = anchors_df[anchors_df["Link Position"].isin(selected_link_pos)]

    # Merge data
    merged_df = anchors_df.merge(pages_df[["URL", "Funnel", "Topic"]], left_on='From', right_on='URL', how='left')
    merged_df = merged_df.rename(columns={"Funnel": "From_Funnel", "Topic": "From_Topic"}).drop(columns=["URL"])
    merged_df = merged_df.merge(pages_df[["URL", "Funnel", "Topic"]], left_on='To', right_on='URL', how='left')
    merged_df = merged_df.rename(columns={"Funnel": "To_Funnel", "Topic": "To_Topic"}).drop(columns=["URL"])

    filtered_urls = set(filtered_pages["URL"])
    global_merged = merged_df[(merged_df["From"].isin(filtered_urls)) & (merged_df["To"].isin(filtered_urls))]

    tabs = st.tabs(["Gap Analysis", "Network Graph", "Sankey", "Topic Heatmap", "Anchors", "Anchor Usage"])

    with tabs[0]:
        st.header("Link Gap Finder")
        inbound_counts = anchors_df.groupby("To")["Anchor Text"].count().reset_index(name="InboundLinks")
        gap_df = filtered_pages.merge(inbound_counts, left_on="URL", right_on="To", how='left')
        gap_df["InboundLinks"] = gap_df["InboundLinks"].fillna(0).astype(int)
        threshold = st.slider("Max inbound links", 0, int(gap_df["InboundLinks"].max()), 2)
        st.dataframe(gap_df[gap_df["InboundLinks"] <= threshold][["URL", "Funnel", "Topic", "Geo", "InboundLinks"]])

    with tabs[1]:
        st.header("Internal Link Graph")
        G = nx.from_pandas_edgelist(global_merged, source='From', target='To', edge_attr='Anchor Text', create_using=nx.DiGraph())
        pos = nx.spring_layout(G)
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
        sankey_data = global_merged.groupby(["From_Funnel", "To_Funnel"]).size().reset_index(name="Count")
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
        topic_counts = global_merged.groupby(["From_Topic", "To_Topic"]).size().reset_index(name="Count")
        heatmap = topic_counts.pivot("From_Topic", "To_Topic", "Count").fillna(0)
        fig = px.imshow(heatmap, text_auto=True, labels=dict(x="To", y="From", color="Links"))
        st.plotly_chart(fig)

    with tabs[4]:
        st.header("Anchor Word Cloud & Top Words")
        col1, col2 = st.columns(2)
        with col1:
            text = " ".join(anchors_df["Anchor Text"].dropna().astype(str))
            wc = WordCloud(width=800, height=400, background_color="white").generate(text)
            fig_wc, ax = plt.subplots(figsize=(8, 4))
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig_wc)
        with col2:
            words = nltk.word_tokenize(text)
            words = [word.lower() for word in words if word.isalpha()]
            counter = Counter(words)
            df_common = pd.DataFrame(counter.most_common(20), columns=["Word", "Frequency"])
            st.bar_chart(df_common.set_index("Word"))

    with tabs[5]:
        st.header("Anchor Usage Frequency")
        usage = anchors_df.groupby("Anchor Text").agg(Occurrences=("Anchor Text", "count"), UniquePages=("To", pd.Series.nunique)).reset_index()
        st.dataframe(usage.sort_values("Occurrences", ascending=False).head(20))

else:
    st.info("Please upload both required files to begin.")
