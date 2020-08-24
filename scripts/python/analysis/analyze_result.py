import time
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from queue import Queue
import threading
import sys
import math


class AnalyzeResult(object):

    def __init__(self, uri, user, password, database_name):
        print("Started")
        #self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        #self._users_queue = Queue()
        #self._print_lock = threading.Lock()
        #self._database_name = database_name

    def analyze(self, file):
        overlap_statistics = []
        overlap_statistics_p = []
        for chunk in pd.read_csv(file,
                                 header=0,
                                 delimiter='\t',
                                 converters={'prodSim': CustomParser, 'prodClicks': CustomParser},
                                 chunksize=10 ** 6):
            df = chunk
            for row in df.itertuples():
                try:
                    if row:
                        search_term = row.searchTerm
                        prod_sims = row.prodSim
                        prod_clicks = row.prodClicks
                        i = 0
                        results = []
                        for prod in prod_clicks:
                            i += 1
                            j = 0
                            for prod_2 in prod_sims:
                                j += 1
                                if prod_2['productId'] == prod['productId']:
                                    result = {'product_id': prod_2['productId'], 'click_pos': i, 'sim_pos': j, 'occurrences': prod['occurrences'], 'sim': prod_2['sim']}
                                    results.append(result)
                                    break
                        overlap_statistics.append(results.__len__())
                        if prod_clicks.__len__() == 10:
                            overlap_statistics_p.append(results.__len__()/prod_clicks.__len__())
                        print("ST:", results.__len__(), prod_clicks.__len__(), search_term, results)

                except Exception as e:
                    print(e, row)
        print("Average Overlap:", np.average(np.array(overlap_statistics)), "over", overlap_statistics_p.__len__())
        print("Average Overlap in percentage:", np.average(np.array(overlap_statistics_p)))

def CustomParser(data):
    import json
    try:
        j1 = json.loads(data)
    except Exception as e:
        print(e, data)
        j1 = []
    return j1

if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    analyzer = AnalyzeResult(uri=uri, user="neo4j", password="pippo1", database_name="test-embeddings-2")
    analyzer.analyze("analysis/analysis_yearly_random_t15_purchasedonly_10_10000.txt")
    end = time.time() - start
    print("Time to complete:", end)


