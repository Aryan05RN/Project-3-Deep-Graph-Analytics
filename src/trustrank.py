import os
import pandas as pd
from neo4j import GraphDatabase


class TrustRankScorer:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def _get_seed_sets(self, session, transactions_csv="data/transactions.csv"):
        """Identifies known fraud seed accounts and verified clean hub accounts for TrustRank."""
        # 1. Extract known fraud-linked accounts from Neo4j (with CSV fallback)
        fraud_rows = session.run("""
            MATCH (a:Account)-[:SENT|RECEIVED_BY]-(t:Transaction)
            WHERE toString(t.is_fraud) IN ['1', 'true', 'True']
               OR toString(t.is_synthetic_fraud) IN ['1', 'true', 'True']
            RETURN DISTINCT toString(a.id) AS account_id
        """).data()
        fraud_seeds = {str(r["account_id"]) for r in fraud_rows}

        if not fraud_seeds and os.path.exists(transactions_csv):
            tx_df = pd.read_csv(transactions_csv)
            fraud_tx = tx_df[tx_df["is_synthetic_fraud"].astype(bool)]
            fraud_seeds = set(fraud_tx["sender_account"].astype(str)).union(
                set(fraud_tx["receiver_account"].astype(str))
            )

        # 2. Extract top 50 clean, high-volume legitimate accounts as TrustRank seeds
        clean_rows = session.run("""
            MATCH (a:Account)-[r:SENT|RECEIVED_BY]-(:Transaction)
            WHERE NOT toString(a.id) IN $fraud_ids
            RETURN toString(a.id) AS account_id, count(r) AS tx_volume
            ORDER BY tx_volume DESC
            LIMIT 50
        """, fraud_ids=list(fraud_seeds)).data()
        trusted_seeds = [str(r["account_id"]) for r in clean_rows]

        return list(fraud_seeds), trusted_seeds

    def compute_scores(self, output_path="data/trustrank_scores.csv"):
        """Runs Global PageRank, Fraud Personalized PageRank, and TrustRank via Neo4j GDS."""
        with self.driver.session() as session:
            # 1. Clean up any existing projection
            session.run("CALL gds.graph.drop('fraudGraph', false)")

            # 2. Project Account -> Account money-flow topology
            print("Projecting transaction graph for PageRank & TrustRank algorithms...")
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(target:Account)
                WITH gds.graph.project('fraudGraph', a, target) AS g
                RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """)

            fraud_seeds, trusted_seeds = self._get_seed_sets(session)
            print(f"Loaded {len(fraud_seeds)} fraud seeds and {len(trusted_seeds)} verified TrustRank hubs.")

            # 3. Standard Global PageRank
            print("1/3 Running Global PageRank...")
            global_pr = session.run("""
                CALL gds.pageRank.stream('fraudGraph', {
                    maxIterations: 20,
                    dampingFactor: 0.85
                })
                YIELD nodeId, score
                RETURN toString(gds.util.asNode(nodeId).id) AS account_id, score AS global_pagerank
            """).data()

            # 4. Fraud-Seeded Personalized PageRank (Contagion Risk)
            print("2/3 Running Fraud-Seeded Personalized PageRank...")
            if fraud_seeds:
                fraud_ppr = session.run("""
                    MATCH (seed:Account)
                    WHERE toString(seed.id) IN $seed_ids
                    WITH collect(seed) AS sourceNodes
                    CALL gds.pageRank.stream('fraudGraph', {
                        maxIterations: 20,
                        dampingFactor: 0.85,
                        sourceNodes: sourceNodes
                    })
                    YIELD nodeId, score
                    RETURN toString(gds.util.asNode(nodeId).id) AS account_id, score AS fraud_ppr_score
                """, seed_ids=fraud_seeds).data()
            else:
                fraud_ppr = [{"account_id": r["account_id"], "fraud_ppr_score": r["global_pagerank"]} for r in global_pr]

            # 5. TrustRank (Legitimate Reputation Propagation)
            print("3/3 Running TrustRank Reputation Scoring...")
            if trusted_seeds:
                trust_pr = session.run("""
                    MATCH (hub:Account)
                    WHERE toString(hub.id) IN $trust_ids
                    WITH collect(hub) AS sourceNodes
                    CALL gds.pageRank.stream('fraudGraph', {
                        maxIterations: 20,
                        dampingFactor: 0.85,
                        sourceNodes: sourceNodes
                    })
                    YIELD nodeId, score
                    RETURN toString(gds.util.asNode(nodeId).id) AS account_id, score AS trustrank_score
                """, trust_ids=trusted_seeds).data()
            else:
                trust_pr = [{"account_id": r["account_id"], "trustrank_score": r["global_pagerank"]} for r in global_pr]

            # 6. Drop in-memory GDS projection
            session.run("CALL gds.graph.drop('fraudGraph', false)")

        # Combine all three graph centrality scores into a unified feature table
        df_global = pd.DataFrame(global_pr)
        df_fraud = pd.DataFrame(fraud_ppr)
        df_trust = pd.DataFrame(trust_pr)

        scores_df = (
            df_global.merge(df_fraud, on="account_id", how="left")
            .merge(df_trust, on="account_id", how="left")
            .fillna(0.0)
        )

        # Normalize scores and calculate Net Fraud Risk Index (high fraud PPR + low TrustRank)
        max_fraud = scores_df["fraud_ppr_score"].max() or 1.0
        max_trust = scores_df["trustrank_score"].max() or 1.0
        scores_df["net_risk_index"] = (
            (scores_df["fraud_ppr_score"] / max_fraud)
            - 0.5 * (scores_df["trustrank_score"] / max_trust)
        ).round(6)

        scores_df = scores_df.sort_values(by="net_risk_index", ascending=False).reset_index(drop=True)
        scores_df.to_csv(output_path, index=False)

        print(f"\n[SUCCESS] Saved PageRank & TrustRank scores for {len(scores_df):,} accounts -> {output_path}")
        print("\n--- Top 10 High-Risk / Low-Trust Accounts ---")
        print(scores_df.head(10).to_string(index=False))


if __name__ == "__main__":
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"

    print("Connecting to Neo4j GDS for PageRank & TrustRank Risk Scoring...\n")
    scorer = TrustRankScorer(URI, USER, PASSWORD)
    scorer.compute_scores()
    scorer.close()