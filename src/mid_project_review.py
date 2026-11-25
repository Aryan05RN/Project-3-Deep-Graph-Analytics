import os
import pandas as pd
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
from sklearn.decomposition import PCA


class MidProjectReviewPipeline:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def verify_property_graph(self):
        """1. Validates persistent property graph node and relationship counts."""
        with self.driver.session() as session:
            node_counts = session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count"
            ).data()
            edge_counts = session.run(
                "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count"
            ).data()

        print("=== [CHECK 1] Persistent Property Graph Verification ===")
        for row in node_counts:
            print(f"  Node [{row['label']}]: {row['count']:,}")
        for row in edge_counts:
            print(f"  Edge [{row['type']}]: {row['count']:,}")
        return node_counts, edge_counts

    def extract_louvain_clusters(self, output_csv="data/louvain_communities.csv"):
        """2. Runs Louvain community detection and exports cluster assignments."""
        with self.driver.session() as session:
            session.run("CALL gds.graph.drop('fraudGraph', false)")
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(target:Account)
                WITH gds.graph.project('fraudGraph', a, target) AS g
                RETURN g.graphName
            """)
            clusters = session.run("""
                CALL gds.louvain.stream('fraudGraph')
                YIELD nodeId, communityId
                RETURN gds.util.asNode(nodeId).id AS account_id, communityId
                ORDER BY account_id ASC
            """).data()
            session.run("CALL gds.graph.drop('fraudGraph', false)")

        louvain_df = pd.DataFrame(clusters)
        community_sizes = louvain_df["communityId"].value_counts().to_dict()
        louvain_df["community_size"] = louvain_df["communityId"].map(community_sizes)
        louvain_df.to_csv(output_csv, index=False)

        print("\n=== [CHECK 2] Macro-Level Louvain Community Clusters ===")
        print(f"  Total Communities Identified: {louvain_df['communityId'].nunique():,}")
        print(f"  Largest Cluster Size: {louvain_df['community_size'].max()} accounts")
        print(f"  Exported cluster mappings -> {output_csv}")
        return louvain_df

    def validate_embeddings_and_plot(
        self,
        louvain_df,
        embeddings_path="data/node_embeddings.csv",
        transactions_path="data/transactions.csv",
        chart_path="reports/mid_project_diagnostics.png",
    ):
        """3. Validates Node2Vec vectors and generates Mid-Project Review diagnostic visuals."""
        emb_df = pd.read_csv(embeddings_path)
        tx_df = pd.read_csv(transactions_path)

        emb_cols = [c for c in emb_df.columns if c.startswith("emb_")]
        null_count = int(emb_df[emb_cols].isna().sum().sum())

        print("\n=== [CHECK 3] Node2Vec Structural Embeddings Validation ===")
        print(f"  Accounts Embedded: {len(emb_df):,} | Dimensions: {len(emb_cols)}D | Missing Values: {null_count}")

        # Tag accounts involved in synthetic fraud rings
        fraud_tx = tx_df[tx_df["is_synthetic_fraud"].astype(bool)]
        fraud_accounts = set(fraud_tx["sender_account"]).union(set(fraud_tx["receiver_account"]))
        emb_df["is_fraud_ring"] = emb_df["account_id"].isin(fraud_accounts)

        # 2D PCA projection of 16D embeddings
        pca = PCA(n_components=2, random_state=42)
        coords = pca.fit_transform(emb_df[emb_cols])
        emb_df["pc1"] = coords[:, 0]
        emb_df["pc2"] = coords[:, 1]

        # Build 2-panel diagnostic visual
        os.makedirs(os.path.dirname(chart_path), exist_ok=True)
        fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

        # Panel 1: Top 10 Louvain Community Sizes
        top_clusters = (
            louvain_df.groupby("communityId")["account_id"]
            .count()
            .sort_values(ascending=False)
            .head(10)
        )
        axes[0].bar(
            [f"C-{cid}" for cid in top_clusters.index],
            top_clusters.values,
            color="#2B5C8F",
        )
        axes[0].set_title("Top 10 Louvain Account Communities")
        axes[0].set_xlabel("Community ID")
        axes[0].set_ylabel("Account Count")
        axes[0].tick_params(axis="x", rotation=45)
        axes[0].grid(axis="y", alpha=0.25)

        # Panel 2: Node2Vec 2D PCA Scatter
        normal = emb_df[~emb_df["is_fraud_ring"]]
        fraud = emb_df[emb_df["is_fraud_ring"]]
        axes[1].scatter(normal["pc1"], normal["pc2"], alpha=0.4, s=18, color="#4C72B0", label="Normal Accounts")
        axes[1].scatter(fraud["pc1"], fraud["pc2"], alpha=0.9, s=42, color="#DD3434", label="Fraud Ring Accounts")
        axes[1].set_title("Node2Vec 16D Embeddings (2D PCA Projection)")
        axes[1].set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0]:.1%} var)")
        axes[1].set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1]:.1%} var)")
        axes[1].legend()
        axes[1].grid(True, alpha=0.25)

        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()
        print(f"\n[SUCCESS] Mid-Project Review diagnostic visual saved -> {chart_path}")


if __name__ == "__main__":
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"

    print("Running Mid-Project Review Analytics & Diagnostic Pipeline...\n")
    pipeline = MidProjectReviewPipeline(URI, USER, PASSWORD)
    pipeline.verify_property_graph()
    clusters_df = pipeline.extract_louvain_clusters()
    pipeline.validate_embeddings_and_plot(clusters_df)
    pipeline.close()