import time
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from queue import Queue
import threading
import sys
import math


class AOLDatasetImporter(object):

    def __init__(self, uri, user, password, database_name):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self._users_queue = Queue()
        self._print_lock = threading.Lock()
        self._database_name = database_name


    def close(self):
        self._driver.close()

    def import_event_data(self, file):
        with self._driver.session(database=self._database_name) as session:
            self.executeNoException(session, "CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")
            self.executeNoException(session, "CREATE CONSTRAINT ON (i:SearchQuery) ASSERT i.query IS UNIQUE")
            self.executeNoException(session, "CREATE CONSTRAINT ON (l:ClickedURL) ASSERT l.url IS UNIQUE")

            dtype = {"AnonID": np.int64}
            j = 0;
            for chunk in pd.read_csv(file,
                                     header=0,
                                     delimiter='\t',
                                     dtype=dtype,
                                     names=['AnonID', 'Query', 'QueryTime', 'ItemRank', 'ClickURL'],
                                     parse_dates=['QueryTime'],
                                     chunksize=10 ** 6):
                df = chunk
                tx = session.begin_transaction()
                i = 0;
                query_with_url = """
                    MERGE (user:User {userId: $userId})    
                    CREATE (se:SearchEvent {timestamp: $timestamp})
                    MERGE (search:SearchQuery {query: $query})
                    MERGE (user)-[:PERFORMS]->(se)
                    MERGE (se)-[:HAS_QUERY]->(search)    
                    MERGE (url:ClickedURL {url: $url})
                    MERGE (se)-[:HAS_CLICK {rank: $rank}]->(url)
                """
                query_without_url = """
                    MERGE (user:User {userId: $userId})    
                    CREATE (se:SearchEvent {timestamp: $timestamp})
                    MERGE (search:SearchQuery {query: $query})
                    MERGE (user)-[:PERFORMS]->(se)
                    MERGE (se)-[:HAS_QUERY]->(search)
                """

                for row in df.itertuples():
                    try:
                        if row:
                            user_id = row.AnonID
                            query = row.Query
                            timestamp = row.QueryTime.to_pydatetime()
                            rank = row.ItemRank
                            url = row.ClickURL
                            if not math.isnan(rank) :
                                tx.run(query_with_url, {"userId": user_id,
                                                        "timestamp": timestamp,
                                                        "query": query,
                                                        "url": url,
                                                        "rank": rank})
                            else:
                                tx.run(query_without_url, {"userId": user_id,
                                                           "timestamp": timestamp,
                                                           "query": query})
                            i += 1
                            j += 1
                            if i == 1000:
                                tx.commit()
                                print(j, "lines processed")
                                i = 0
                                tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row)
                tx.commit()
                print(j, "lines processed")
            print(j, "lines processed")

    def process_users(self):
        for k in range(50):
            print("starting thread: ", k)
            user_info_thread = threading.Thread(target=self.create_has_next)
            user_info_thread.daemon = True
            user_info_thread.start()

        get_users_query = """
            MATCH (user:User)
            RETURN user.userId as userId
        """

        with self._driver.session(database=self._database_name) as session:
            i = 0
            for user in session.run(get_users_query):
                user_id = user["userId"]
                self._users_queue.put(user_id)
                i += 1
                if i % 1000 == 0:
                    print(i, "lines processed")
            print(i, "lines processed")
            self._users_queue.join()
            print("Done")

    def create_has_next(self):
        query = """
            MATCH (user:User {userId: $userId})-[:PERFORMS]->(se:SearchEvent)
            WITH user, se
            ORDER BY se.timestamp
            WITH user, collect(se) as ses
            UNWIND range(0, size(ses) - 2) as index
            WITH user, ses[index] as source, ses[index+1] as dest
            MERGE (source)-[:HAS_NEXT]->(dest)
        """
        while True:
            user_id = self._users_queue.get()
            with self._driver.session(database=self._database_name) as session:
                try:
                    session.run(query, {"userId":user_id})
                except Exception as e:
                    print(user_id, e)
            self._users_queue.task_done()

    def executeNoException(self, session, query):
        try:
            session.run(query)
        except Exception as e:
            pass


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = AOLDatasetImporter(uri=uri, user="neo4j", password="pippo1", database_name="test-embeddings-2")
    base_path = "/Users/ale/neo4j-servers/embeddings/datasets/AOL-user-ct-collection"
    if (len(sys.argv) > 1):
        base_path = sys.argv[1]
    #importing.import_event_data(file=base_path + "/user-ct-test-collection-01.txt")
    importing.process_users()
    end = time.time() - start
    importing.close()
    print("Time to complete:", end)
