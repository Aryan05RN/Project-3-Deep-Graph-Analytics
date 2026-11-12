# src/graph_schema.py

def get_constraint_queries():
    """
    Defines the unique constraints for our graph database nodes.
    This ensures that we never have duplicate users, accounts, or transactions.
    """
    return [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Device) REQUIRE d.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:IP_Address) REQUIRE i.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE;"
    ]

def get_edge_mapping_rules():
    """
    Documents the property graph edge relationships for the analytics engine.
    """
    return {
        "HAS_ACCOUNT": "(User)-[:HAS_ACCOUNT]->(Account)",
        "USES_DEVICE": "(User)-[:USES_DEVICE]->(Device)",
        "LOGS_IN_FROM": "(User)-[:LOGS_IN_FROM]->(IP_Address)",
        "TRANSFERRED_TO": "(Account)-[:TRANSFERRED_TO {amount, timestamp, tx_id}]->(Account)"
    }

if __name__ == "__main__":
    print("[SUCCESS] Graph Property Schema Initialized.")
    print("\nNode Constraints to Apply:")
    for query in get_constraint_queries():
        print(f" -> {query}")
        
    print("\nEdge Topology:")
    for name, topology in get_edge_mapping_rules().items():
        print(f" -> {name}: {topology}")