import json
import os
import networkx as nx
import pandas as pd
from neo4j import GraphDatabase


class CycleAndFlowAnalyzer:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def build_flow_graph(self):
        """Extracts directed Account -> Account transaction flows from Neo4j into NetworkX."""
        with self.driver.session() as session:
            all_accounts = session.run(
                "MATCH (a:Account) RETURN DISTINCT toString(a.id) AS account_id"
            ).data()
            edge_rows = session.run("""
                MATCH (src:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account)
                WHERE src <> dst
                RETURN toString(src.id) AS source,
                       toString(dst.id) AS target,
                       count(t) AS tx_count,
                       sum(coalesce(toFloat(t.amount), 0.0)) AS total_amount
            """).data()

        G = nx.DiGraph()
        for row in all_accounts:
            G.add_node(row["account_id"])

        for row in edge_rows:
            G.add_edge(
                row["source"],
                row["target"],
                tx_count=int(row["tx_count"]),
                capacity=float(row["total_amount"]),
            )

        print(f"Loaded directed money-flow graph: {G.number_of_nodes():,} accounts | {G.number_of_edges():,} directed corridors")
        return G

    def _find_bounded_cycles(self, G, max_length=6, max_cycles=500):
        """Finds simple directed cycles of length 3 to max_length within non-trivial SCCs."""
        sccs = [c for c in nx.strongly_connected_components(G) if len(c) >= 3]
        detected_cycles = []
        seen_signatures = set()

        for comp in sccs:
            subG = G.subgraph(comp)
            for start_node in comp:
                if len(detected_cycles) >= max_cycles:
                    break
                stack = [(start_node, [start_node])]
                while stack and len(detected_cycles) < max_cycles:
                    curr, path = stack.pop()
                    for nbr in subG.successors(curr):
                        if nbr == start_node and len(path) >= 3:
                            min_idx = path.index(min(path))
                            canonical = tuple(path[min_idx:] + path[:min_idx])
                            if canonical not in seen_signatures:
                                seen_signatures.add(canonical)
                                detected_cycles.append(list(canonical))
                        elif nbr not in path and len(path) < max_length:
                            stack.append((nbr, path + [nbr]))

        return sccs, detected_cycles

    def analyze_cycles_and_flows(
        self,
        features_csv="data/network_flow_features.csv",
        cycles_json="data/detected_cycles.json",
    ):
        """Computes circular laundering metrics, max-flow capacities, and mule pass-through ratios."""
        G = self.build_flow_graph()

        print("Running Tarjan SCC & bounded cycle detection (3-6 hop laundering rings)...")
        sccs, cycles = self._find_bounded_cycles(G, max_length=6, max_cycles=500)

        # Map SCC membership and cycle participation counts per account
        node_scc_size = {}
        scc_bottleneck_flow = {}

        for idx, comp in enumerate(sccs):
            comp_list = list(comp)
            subG = G.subgraph(comp_list)
            # Estimate max-flow capacity across opposite ends of each SCC ring
            src_node, sink_node = comp_list[0], comp_list[-1]
            try:
                bottleneck = nx.maximum_flow_value(subG, src_node, sink_node, capacity="capacity")
            except nx.NetworkXError:
                bottleneck = 0.0

            for node in comp_list:
                node_scc_size[node] = len(comp_list)
                scc_bottleneck_flow[node] = round(float(bottleneck), 2)

        cycle_counts = {n: 0 for n in G.nodes()}
        min_cycle_len = {n: 0 for n in G.nodes()}
        structured_rings = []

        for ring_idx, ring in enumerate(cycles, start=1):
            ring_len = len(ring)
            ring_edges = []
            ring_volume = 0.0

            for i in range(ring_len):
                u = ring[i]
                v = ring[(i + 1) % ring_len]
                edge_data = G.get_edge_data(u, v, default={"capacity": 0.0, "tx_count": 1})
                amt = float(edge_data["capacity"])
                ring_volume += amt
                ring_edges.append({
                    "source": u,
                    "target": v,
                    "amount": round(amt, 2),
                    "tx_count": int(edge_data["tx_count"]),
                })

            for node in ring:
                cycle_counts[node] += 1
                if min_cycle_len[node] == 0 or ring_len < min_cycle_len[node]:
                    min_cycle_len[node] = ring_len

            structured_rings.append({
                "ring_id": f"RING-{ring_idx:03d}",
                "hop_length": ring_len,
                "total_volume": round(ring_volume, 2),
                "accounts": ring,
                "edges": ring_edges,
            })

        # Sort rings by total laundered volume for Week 4 Investigator Dashboard
        structured_rings.sort(key=lambda r: r["total_volume"], reverse=True)

        # Compute directional flow & mule pass-through features for every account
        records = []
        for node in sorted(G.nodes()):
            inflow = sum(data["capacity"] for _, _, data in G.in_edges(node, data=True))
            outflow = sum(data["capacity"] for _, _, data in G.out_edges(node, data=True))
            max_side = max(inflow, outflow)
            pass_through_ratio = (min(inflow, outflow) / max_side) if max_side > 0 else 0.0

            records.append({
                "account_id": node,
                "in_scc_ring": 1 if node in node_scc_size else 0,
                "scc_size": node_scc_size.get(node, 1),
                "cycle_count": cycle_counts[node],
                "min_cycle_length": min_cycle_len[node],
                "total_inflow": round(inflow, 2),
                "total_outflow": round(outflow, 2),
                "flow_pass_through_ratio": round(pass_through_ratio, 4),
                "ring_bottleneck_flow": scc_bottleneck_flow.get(node, 0.0),
            })

        flow_df = pd.DataFrame(records)
        flow_df.to_csv(features_csv, index=False)

        os.makedirs(os.path.dirname(cycles_json), exist_ok=True)
        with open(cycles_json, "w", encoding="utf-8") as f:
            json.dump(structured_rings[:50], f, indent=2)

        print(f"\n=== Circular Laundering & Network Flow Summary ===")
        print(f"  Strongly Connected Components (size >= 3): {len(sccs)}")
        print(f"  Circular Laundering Loops Isolated: {len(cycles)}")
        print(f"  Accounts Flagged in Cycles: {(flow_df['cycle_count'] > 0).sum():,}")
        print(f"  Saved flow features -> {features_csv}")
        print(f"  Saved top circular ring topologies -> {cycles_json}")

        top_cyclical = flow_df.sort_values(
            by=["cycle_count", "flow_pass_through_ratio", "total_outflow"],
            ascending=False,
        ).head(10)
        print("\n--- Top 10 Accounts by Circular Laundering Participation & Pass-Through Ratio ---")
        print(top_cyclical[["account_id", "scc_size", "cycle_count", "min_cycle_length", "flow_pass_through_ratio", "total_outflow"]].to_string(index=False))


if __name__ == "__main__":
    URI = "bolt://127.0.0.1:7687"
    USER = "neo4j"
    PASSWORD = "password123"

    print("Connecting to Neo4j for Cycle Detection & Network Flow Analysis...\n")
    analyzer = CycleAndFlowAnalyzer(URI, USER, PASSWORD)
    analyzer.analyze_cycles_and_flows()
    analyzer.close()