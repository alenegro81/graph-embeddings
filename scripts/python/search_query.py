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
        WITH DISTINCT query.query AS query, query.embeddingGGVecT1 AS embedding, url.url as url, count(se) as searchEvents
        ORDER by searchEvents desc
        RETURN url, query as queryText, embedding
        Limit 5000
    """
    query_old_1 = """
        MATCH (query:SearchTerm)-[:WAS_USED_IN]->(:SessionEntry)
        WITH query, count(*) as occurrencies
        ORDER BY occurrencies desc
        LIMIT 1000
        RETURN query.searchTerm as queryText, query.embeddingGGVecT1 as embedding, 'product' as productName 
    """

    query = """
        MATCH (item:TargetItem_t3)-[r:UNWEIGHTED_CLICKED_AFTER_SEARCH_2|ADDED_TO_CART_AFTER_SEARCH|PURCHASED_AFTER_SEARCH]->(s:TargetSearchTerm_t3)
        WITH item, sum(r.numberOfTimes) as occurrencies
        ORDER BY occurrencies desc
        LIMIT 1000
        MATCH (item)-[:BELONGS_TO_CATEGORY]->(category:Category)
        RETURN item.oms_sku as productId, item.embeddingNode2vecT2 as embedding, category.name as categoryName
    """

    result = session.run(query)

    X = pd.DataFrame([dict(record) for record in result])

X_embedded = TSNE(n_components=2, random_state=6).fit_transform(list(X.embedding))

products = X.productId
df = pd.DataFrame(data = {
    "product": products,
    "category": X.categoryName,
    "x": [value[0] for value in X_embedded],
    "y": [value[1] for value in X_embedded]
})

chart = alt.Chart(df).mark_circle(size=60).encode(
    x='x',
    y='y',
    color='category',
    tooltip=['product', 'category']
).properties(width=700, height=400)

chart.save("test_python_1.html")
altair_viewer.show(chart)

#print("Just wait")