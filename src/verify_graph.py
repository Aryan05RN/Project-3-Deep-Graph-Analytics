from neo4j import GraphDatabase

def verify_database():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password123"

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            # Verify Node Topology
            node_result = session.run("MATCH (n) RETURN labels(n) AS label, count(*) AS count")
            print("--- Graph Node Summary ---")
            for record in node_result:
                print(f"Node '{record['label'][0]}': {record['count']} total")

            # Verify Edge Topology
            edge_result = session.run("MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count")
            print("\n--- Graph Edge Summary ---")
            for record in edge_result:
                print(f"Edge '{record['type']}': {record['count']} total")

        driver.close()
        print("\n[SUCCESS] Graph topology mapped and verified successfully.")
    except Exception as e:
        print(f"[ERROR] Could not verify database: {e}")

if __name__ == "__main__":
    print("Initiating Structural Sanity Check...\n")
    verify_database()