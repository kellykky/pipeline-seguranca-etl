import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('pt_BR')

def generate_access_logs(n=5000):
    users = ['admin', 'root', 'svc_backup', 'deploy_bot', 'analyst_01', 'dev_user']
    systems = ['servidor-bd-prod', 'firewall-01', 'vpn-gateway', 'server-api', 'storage-nas']
    actions = ['SSH_LOGIN', 'SUDO_EXEC', 'FILE_ACCESS', 'CONFIG_CHANGE', 'LOGIN_FAILED']

    logs = []
    base_time = datetime.now() - timedelta(days=30)

    for _ in range(n):
        success = random.random() > 0.15  # 15% de falhas
        logs.append({
            "timestamp": base_time + timedelta(
                seconds=random.randint(0, 30 * 24 * 3600)
            ),
            "user": random.choice(users),
            "system": random.choice(systems),
            "action": random.choice(actions),
            "ip_address": fake.ipv4(),
            "success": success,
            "duration_sec": random.randint(1, 300) if success else 0
        })

    df = pd.DataFrame(logs).sort_values("timestamp")
    df.to_csv("data/raw/access_logs.csv", index=False)
    print(f"{len(df)} logs gerados em data/raw/access_logs.csv")
    return df

if __name__ == "__main__":
    generate_access_logs()