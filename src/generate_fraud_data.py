import os
import random
import pandas as pd
import numpy as np

def generate_graph_data(num_users=500, num_transactions=2000, seed=42):
    np.random.seed(seed)
    random.seed(seed)
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # 1. Generate Nodes (Entities)
    users = [f"USR_{i:04d}" for i in range(1, num_users + 1)]
    accounts = [f"ACC_{i:04d}" for i in range(1, int(num_users * 1.2) + 1)]
    devices = [f"DEV_{i:04d}" for i in range(1, int(num_users * 0.4) + 1)]
    ips = [f"IP_{i:04d}" for i in range(1, int(num_users * 0.3) + 1)]
    
    # 2. Map Users to Accounts, Devices, and IPs
    user_acc_links = [{"user_id": u, "account_id": random.choice(accounts)} for u in users]
    
    user_device_links = []
    user_ip_links = []
    for u in users:
        # Simulate shared infrastructure (10% chance multiple users share the exact same device/IP)
        dev = "DEV_0001" if random.random() < 0.10 else random.choice(devices)
        ip = "IP_0001" if random.random() < 0.12 else random.choice(ips)
        user_device_links.append({"user_id": u, "device_id": dev})
        user_ip_links.append({"user_id": u, "ip_address": ip})
        
    # 3. Generate Standard Transactions
    tx_list = []
    for t_id in range(1, num_transactions + 1):
        sender = random.choice(accounts)
        receiver = random.choice([a for a in accounts if a != sender])
        amount = round(float(np.random.exponential(scale=250.0) + 10), 2)
        tx_list.append({
            "tx_id": f"TX_{t_id:06d}",
            "sender_account": sender,
            "receiver_account": receiver,
            "amount": amount,
            "is_synthetic_fraud": False
        })
        
    # 4. Inject a Syndicated Fraud Ring (Circular loops: ACC_A -> ACC_B -> ACC_C -> ACC_A)
    fraud_ring_accounts = ["ACC_0001", "ACC_0002", "ACC_0003", "ACC_0004"]
    for i in range(20):  
        sender = fraud_ring_accounts[i % len(fraud_ring_accounts)]
        receiver = fraud_ring_accounts[(i + 1) % len(fraud_ring_accounts)]
        tx_list.append({
            "tx_id": f"TX_FRAUD_{i:04d}",
            "sender_account": sender,
            "receiver_account": receiver,
            "amount": round(float(random.randint(4900, 5000)), 2),
            "is_synthetic_fraud": True
        })
        
    # 5. Export to CSV
    pd.DataFrame(user_acc_links).to_csv('data/user_accounts.csv', index=False)
    pd.DataFrame(user_device_links).to_csv('data/user_devices.csv', index=False)
    pd.DataFrame(user_ip_links).to_csv('data/user_ips.csv', index=False)
    pd.DataFrame(tx_list).to_csv('data/transactions.csv', index=False)
    
    print(f"[SUCCESS] Fraud Data generated: {len(users)} Users, {len(accounts)} Accounts, {len(tx_list)} Transactions.")

if __name__ == "__main__":
    generate_graph_data()