import pandas as pd
from neo4j import GraphDatabase

class GraphEmbeddings:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def generate_node2vec_embeddings(self, output_path="data/node_embeddings.csv"):
        """Projects the transaction graph and generates 16D Node2Vec structural embeddings."""
        with self.driver.session() as session:
            # 1. Drop any leftover projection
            session.run("CALL gds.graph.drop('fraudGraph', false)")

            # 2. Project Account -> Account transaction topology
            print("Projecting transaction graph for Node2Vec random walks...")
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(target:Account)
                WITH gds.graph.project('fraudGraph', a, target) AS g
                RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """)

            # 3. Execute Node2Vec algorithm and stream vector embeddings
            print("Training Node2Vec structural embeddings...")
            result = session.run("""
                CALL gds.node2vec.stream('fraudGraph', {
                    embeddingDimension: 16,
                    walkLength: 10,
                    iterations: 5,
                    randomSeed: 42
                })
                YIELD nodeId, embedding
                RETURN gds.util.asNode(nodeId).id AS account_id, embedding
                ORDER BY account_id ASC
            """)

            records = [{"account_id": r["account_id"], "embedding": r["embedding"]} for r in result]

            # 4. Clean up in-memory GDS projection
            session.run("CALL gds.graph.drop('fraudGraph', false)")

        if not records:
            print("[WARNING] No embeddings generated. Verify graph data exists.")
            return

        # Expand embedding vectors into tabular columns for Week 3 ML models
        emb_df = pd.DataFrame([r["embedding"] for r in records])
        emb_df.columns = [f"emb_{i}" for i in range(emb_df.shape[1])]
        emb_df.insert(0, "account_id", [r["account_id"] for r in records])

        emb_df.to_csv(output_path, index=False)
        print(f"\n[SUCCESS] Generated {len(emb_df)} embeddings (16 dimensions) -> saved to {output_path}")
        print("\n--- Sample Node2Vec Embeddings (First 5 Accounts, First 4 Dimensions) ---")
        print(emb_df[["account_id", "emb_0", "emb_1", "emb_2", "emb_3"]].head())

if __name__ == "__main__":
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"

    print("Connecting to Neo4j GDS for Node2Vec representation learning...")
    embedder = GraphEmbeddings(URI, USER, PASSWORD)
    embedder.generate_node2vec_embeddings()
    embedder.close()