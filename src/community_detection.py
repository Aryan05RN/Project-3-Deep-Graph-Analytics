from neo4j import GraphDatabase

class CommunityDetection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_louvain(self):
        """Projects the graph via Cypher and runs Louvain community detection to find fraud clusters."""
        with self.driver.session() as session:
            # 1. Clean up any existing projection with the same name
            session.run("CALL gds.graph.drop('fraudGraph', false)")
            
            # 2. Create Cypher graph projection mapping Account -> Transaction -> Account flow
            print("Projecting graph topology for GDS analytics...")
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(target:Account)
                WITH gds.graph.project('fraudGraph', a, target) AS g
                RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """)
            
            # 3. Run Louvain algorithm and stream results
            print("Executing Louvain Community Detection algorithm...")
            result = session.run("""
                CALL gds.louvain.stream('fraudGraph')
                YIELD nodeId, communityId
                RETURN gds.util.asNode(nodeId).id AS account_id, communityId
                ORDER BY communityId ASC
                LIMIT 15
            """)
            
            print("\n--- Louvain Community Detection Results (Sample Clusters) ---")
            for record in result:
                print(f"Account: {record['account_id']} | Community ID: {record['communityId']}")
            
            # 4. Clean up projection
            session.run("CALL gds.graph.drop('fraudGraph', false)")

if __name__ == "__main__":
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"
    
    print("Connecting to Neo4j for advanced graph algorithms...")
    detector = CommunityDetection(URI, USER, PASSWORD)
    detector.run_louvain()
    detector.close()