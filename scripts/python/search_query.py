from neo4j import GraphDatabase
import numpy as np
from sklearn.manifold import TSNE
import altair as alt
import altair_viewer
import pandas as pd


driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "alessandro"))

with driver.session(database="neo4j") as session:
    # result = session.run("""
    #     MATCH (query:SearchQuery)<-[:HAS_QUERY]-()
    #     WHERE size(query.query) > 3
    #     WITH query, count(*) as occurrencies
    #     ORDER BY occurrencies desc
    #     LIMIT 1000
    #     MATCH (url:ClickedURL)<-[r:HAS_CLICK]-(se:SearchEvent)-[:HAS_QUERY]->(query)
    #     WITH DISTINCT query.query AS query, query.embeddingNode2vecT1 AS embedding, r, url.url as url
    #     ORDER BY r.rank asc
    #     RETURN query as queryText, embedding, collect(url)[0] as url
    #     Limit 10000
    # """)

    query_old = """
        MATCH (url:ClickedURL)<-[:HAS_CLICK]-()
        WITH url, count(*) as occurrencies
        ORDER BY occurrencies desc
        LIMIT 30
        MATCH (url)<-[:HAS_CLICK]-(se:SearchEvent)-[:HAS_QUERY]->(query:SearchQuery)
        WITH DISTINCT query.query AS query, query.embeddingNode2vecT4 AS embedding, url.url as url, count(se) as searchEvents
        ORDER by searchEvents desc
        RETURN url, query as queryText, embedding
        Limit 5000
    """
    query = """
        MATCH (query:SearchTerm)-[:WAS_USED_IN]->(:SessionEntry)
        WITH query, count(*) as occurrencies
        ORDER BY occurrencies desc
        LIMIT 100
        RETURN query.searchTerm as queryText, query.embeddingNode2vecT3 as embedding, 'product' as productName 
    """

    result = session.run(query)

    X = pd.DataFrame([dict(record) for record in result])

X_embedded = TSNE(n_components=2, random_state=6).fit_transform(list(X.embedding))

queries = X.queryText
df = pd.DataFrame(data = {
    "query": queries,
    "product": X.productName,
    "x": [value[0] for value in X_embedded],
    "y": [value[1] for value in X_embedded]
})

chart = alt.Chart(df).mark_circle(size=60).encode(
    x='x',
    y='y',
    color='product',
    tooltip=['query', 'product']
).properties(width=700, height=400)

chart.save("test3.html")
#altair_viewer.show(chart)

#print("Just wait")