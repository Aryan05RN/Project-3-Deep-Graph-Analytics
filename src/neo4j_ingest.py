import pandas as pd
from neo4j import GraphDatabase

class GraphDatabaseConnector:
    def __init__(self, uri, user, password):
        """Initializes the connection to the active Neo4j database."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def apply_constraints(self):
        """Applies unique constraints to prevent duplicate nodes."""
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE")
            print("[SUCCESS] Neo4j Constraints applied.")

    def ingest_data(self):
        """Reads local CSVs and pushes nodes and edges to the Graph Database."""
        try:
            user_acc = pd.read_csv('data/user_accounts.csv')
            transactions = pd.read_csv('data/transactions.csv')
            
            with self.driver.session() as session:
                # 1. Ingest Users, Accounts, and HAS_ACCOUNT edges
                print("Ingesting Users and Accounts into Graph (This may take a moment)...")
                session.run("""
                    UNWIND $rows AS row
                    MERGE (u:User {id: row.user_id})
                    MERGE (a:Account {id: row.account_id})
                    MERGE (u)-[:HAS_ACCOUNT]->(a)
                """, parameters={'rows': user_acc.to_dict('records')})
                
                # 2. Ingest Transactions and network flow edges
                print("Ingesting Transactions...")
                session.run("""
                    UNWIND $rows AS row
                    MATCH (sender:Account {id: row.sender_account})
                    MATCH (receiver:Account {id: row.receiver_account})
                    MERGE (t:Transaction {tx_id: row.tx_id})
                    SET t.amount = row.amount, t.is_fraud = row.is_synthetic_fraud
                    MERGE (sender)-[:SENT]->(t)
                    MERGE (t)-[:RECEIVED_BY]->(receiver)
                """, parameters={'rows': transactions.to_dict('records')})
                
            print("[SUCCESS] Property Graph data ingestion complete.")
        except FileNotFoundError:
            print("[ERROR] CSV files not found. Run generate_fraud_data.py first.")
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")

if __name__ == "__main__":
    # Local Neo4j credentials
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "password123" 
    
    print("Connecting to live Neo4j Database...")
    
    # Live Execution Pipeline
    connector = GraphDatabaseConnector(URI, USER, PASSWORD)
    connector.apply_constraints()
    connector.ingest_data()
    connector.close()