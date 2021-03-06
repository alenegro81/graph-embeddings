import time
from neo4j import GraphDatabase
import sys
import optuna
import numpy as np

GRAPH_HELPER = None;
TARGET_RESULTS = {}


class GraphHelper(object):
    def __init__(self, uri, user, password, database_name):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self._database_name = database_name

    def close(self):
        self._driver.close()

    def create_graph_in_memory(self):
        with self._driver.session(database=self._database_name) as session:
            drop_query = """
                call gds.graph.drop("embeddingGraph")
            """
            executeNoException(session, drop_query)

        with self._driver.session(database=self._database_name) as session:
            create_query = """
                call gds.graph.create(
                    'embeddingGraph',
                    ['SearchTerm', 'Item'],
                      {
                    UNWEIGHTED_PURCHASED_AFTER_SEARCH_MULTIPLIED: {
                      type: "UNWEIGHTED_PURCHASED_AFTER_SEARCH_MULTIPLIED",
                      orientation: "UNDIRECTED"
                    },
                    UNWEIGHTED_ADDED_TO_CART_AFTER_SEARCH_MULTIPLIED: {
                      type: "UNWEIGHTED_ADDED_TO_CART_AFTER_SEARCH_MULTIPLIED",
                      orientation: "UNDIRECTED"
                    }
                  });
            """

            result = session.run(create_query)

    def get_target(self):
        with self._driver.session(database=self._database_name) as session:
            result_query = """
                MATCH (query:SearchTerm)-[:WAS_USED_IN]->(:SessionEntry)
                WITH query, count(*) as occurrencies
                ORDER BY occurrencies desc
                LIMIT 10000 
                MATCH (query)<-[r:ADDED_TO_CART_AFTER_SEARCH|PURCHASED_AFTER_SEARCH]-(item:Item)      
                WITH query, item.oms_sku as productId, sum(r.numberOfTimes) as count
                ORDER BY count desc
                WITH query, {productId: productId, occurrences: count} as prodClick
                RETURN query.searchTerm as searchTerm, collect(prodClick)[0..10] as prodClicks
            """
            results = [];
            with self._driver.session(database=self._database_name) as session:
                for result in session.run(result_query):
                    search_term = result["searchTerm"]
                    prod_clicks = result["prodClicks"]
                    results.append({"search_term": search_term, "prod_clicks": prod_clicks})
            return results

    def get_similarity(self, writeProperty):
        with self._driver.session(database=self._database_name) as session:
            result_query = """
                MATCH (query:SearchTerm)-[:WAS_USED_IN]->(:SessionEntry)
                WITH query, count(*) as occurrencies
                ORDER BY occurrencies desc
                LIMIT 10000 
                MATCH (query)<-[r:HAS_RESULT]-(item:Item)
                WITH query.searchTerm as searchTerm, item.oms_sku as productId, gds.alpha.similarity.cosine(query.{}, item.{}) as sim
                ORDER BY sim desc
                WITH searchTerm, {{productId: productId, sim: sim}} as prodSim
                RETURN searchTerm, collect(prodSim)[0..10] as prodSim
            """.format(writeProperty, writeProperty)
            results = [];
            with self._driver.session(database=self._database_name) as session:
                for result in session.run(result_query):
                    search_term = result["searchTerm"]
                    prod_sim = result["prodSim"]
                    results.append({"search_term": search_term, "prod_sim": prod_sim})
            return results

    def compute_emeddings(self, writeProperty, normalizationStrength, first_second_value, forth_value):
        query = """
            CALL gds.alpha.randomProjection.write(
            "embeddingGraph", 
            {{
            embeddingSize: 512,
            maxIterations: 4,
            iterationWeights: [{},{},1.0,{}],
            writeProperty: '{}',
            normalizationStrength: {},
            concurrency: 76,
            writeConcurrency: 76
            }})
        """.format(first_second_value, first_second_value, forth_value, writeProperty, normalizationStrength)
        print("Query check:", query)
        with self._driver.session(database=self._database_name) as session:
            session.run(query)


def executeNoException(session, query):
    try:
        session.run(query)
    except Exception as e:
        pass
    except:
        pass


def optimize():
    GRAPH_HELPER.create_graph_in_memory()
    print("Graph in memory created")
    study = optuna.create_study()
    print("Study started")
    study.optimize(objective, n_trials=100)
    print(study.best_params)


def objective(trial):
    print("Start objective")
    normalization_strength = trial.suggest_uniform('beta', -1.5, 0.5)
    forth_value = trial.suggest_uniform('forth_value', 0.001, 8.0)
    first_second_value = trial.suggest_uniform('first_second_value', 0.0, 1.0)
    print("Current normalization_strength:", normalization_strength)
    write_property = "optimizationTest"
    print("Staring computing embeddings!")
    GRAPH_HELPER.compute_emeddings(write_property, normalization_strength, first_second_value, forth_value);
    print("Starting similarity")
    results = GRAPH_HELPER.get_similarity(write_property);
    print("CompUting error")

    error = compute_error(results, TARGET_RESULTS)
    print("Current error:", error, "with normalization_strength:", normalization_strength)
    return error


def compute_error(sim_results, target_results):
    overlap_statistics = []
    overlap_statistics_p = []
    sim_results_reshaped = {element['search_term']: element['prod_sim'] for element in sim_results}

    for item in target_results:
        search_term = item['search_term']
        prod_clicks = item['prod_clicks']
        prod_sims = sim_results_reshaped[search_term]
        i = 0
        results = []
        for prod in prod_clicks:
            i += 1
            j = 0
            for prod_2 in prod_sims:
                j += 1
                if prod_2['productId'] == prod['productId']:
                    result = {'product_id': prod_2['productId'], 'click_pos': i, 'sim_pos': j,
                              'occurrences': prod['occurrences'], 'sim': prod_2['sim']}
                    results.append(result)
                    break
        overlap_statistics.append(results.__len__())
        if prod_clicks.__len__() == 10:
            overlap_statistics_p.append(results.__len__() / prod_clicks.__len__())
        #print("ST:", results.__len__(), prod_clicks.__len__(), search_term, results)

    print("Average Overlap:", np.average(np.array(overlap_statistics)), "over", overlap_statistics_p.__len__())
    average_accuracy = np.average(np.array(overlap_statistics_p))
    print("Average Overlap in percentage:", average_accuracy)
    return 1 - average_accuracy

if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    graph_helper = GraphHelper(uri=uri, user="neo4j", password="alessandro", database_name="neo4j")
    GRAPH_HELPER = graph_helper
    TARGET_RESULTS = GRAPH_HELPER.get_target();
    print("Target computed:", TARGET_RESULTS.__len__())
    optimize()
    end = time.time() - start
    graph_helper.close()
    print("Time to complete:", end)
