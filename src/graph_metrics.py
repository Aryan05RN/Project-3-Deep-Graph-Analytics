from neo4j import GraphDatabase
import pandas as pd

class ExploratoryAnalytics:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def calculate_node_degree(self):
        """Calculates Node Degree Centrality to find the most active accounts."""
        query = """
        MATCH (a:Account)-[r:SENT|RECEIVED_BY]-()
        RETURN a.id AS account_id, count(r) AS degree
        ORDER BY degree DESC
        LIMIT 10
        """
        with self.driver.session() as session:
            result = session.run(query)
            print("\n--- Top 10 Accounts by Node Degree (Transaction Volume) ---")
            for record in result:
                print(f"Account: {record['account_id']} | Degree: {record['degree']}")

    def calculate_shortest_path(self, acc1, acc2):
        """Finds the shortest transaction path between two accounts."""
        query = """
        MATCH p = shortestPath((a1:Account {id: $acc1})-[:SENT*..5]-(a2:Account {id: $acc2}))
        RETURN length(p) AS path_length
        """
        with self.driver.session() as session:
            result = session.run(query, acc1=acc1, acc2=acc2)
            record = result.single()
            if record and record['path_length'] is not None:
                print(f"\n--- Shortest Path between {acc1} and {acc2} ---")
                print(f"Path Length: {record['path_length']} hops")
            else:
                print(f"\n--- No path found between {acc1} and {acc2} within 5 hops ---")

if __name__ == "__main__":
    # Local Neo4j credentials
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"
    
    print("Connecting to database for exploratory network analytics...")
    
    explorer = ExploratoryAnalytics(URI, USER, PASSWORD)
    
    # 1. Fundamental Network Metrics
    explorer.calculate_node_degree()
    
    # 2. Shortest Path length (Testing accounts from our synthetic fraud ring)
    explorer.calculate_shortest_path("ACC_0001", "ACC_0004")
    
    explorer.close()