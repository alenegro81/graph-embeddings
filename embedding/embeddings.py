import csrgraph as cg
import nodevectors
import time
from queue import Queue
from neo4j import GraphDatabase
import threading



class GraphEmbeddings(object):

    def __init__(self, uri, user, password, database_name, write_property):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self._nodes_queue = Queue()
        self._print_lock = threading.Lock()
        self._database_name = database_name
        self._write_property = write_property


    def compute(self, file):
        G = cg.read_edgelist(f=file, header=0)
        print("File read")
        ggvec_model = nodevectors.GGVec(order=1)
        print("Model Created")
        start = time.time()
        embeddings = ggvec_model.fit_transform(G)
        end = time.time() - start
        print("Embedding obtained:", end)
        print(embeddings[1])
        return embeddings, G.names;

    def store(self, embeddings, names):
        for k in range(50):
            print("starting thread: ", k)
            user_info_thread = threading.Thread(target=self.add_property)
            user_info_thread.daemon = True
            user_info_thread.start()

        for i in range(embeddings.shape[0] - 1):
            self._nodes_queue.put((names[i], embeddings[i]))
            if i % 1000 == 0:
                print(i, "lines processed")

        print(i, "lines processed")
        self._nodes_queue.join()
        print("Done")

    def add_property(self):
        query = """
            MATCH (n)
            WHERE id(n) = $nodeId
            SET n.{} = $embeddings
        """.format(self._write_property)

        while True:
            node_id, embeddings = self._nodes_queue.get()
            queue_size = self._nodes_queue.qsize()
            if queue_size % 100 == 0:
                with self._print_lock:
                    print("Queue size:", queue_size)
            with self._driver.session(database=self._database_name) as session:
                try:
                    session.run(query, {"nodeId":int(node_id), "embeddings": embeddings.tolist()})
                except Exception as e:
                    print(node_id, e)
            self._nodes_queue.task_done()


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    analyzer = GraphEmbeddings(uri=uri, user="neo4j", password="pippo1",
                               database_name="test-embeddings-2",
                               write_property="embeddingGGVecT1")
    embeddings, names = analyzer.compute("test_10000.txt")
    end = time.time() - start
    print("Time to complete:", end)
    start = time.time()
    analyzer.store(embeddings, names)
    end = time.time() - start
    print("Time to complete:", end)