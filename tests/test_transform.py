import pandas as pd
from etl.transform import transform

def test_anomaly_detection():
    """Usuário com 6 falhas seguidas deve ser marcado como anomalia"""
    # mock de dados com falhas em sequência
    data = {"user": ["root"] * 6, "success": [False] * 6,
            "timestamp": pd.date_range("2026-01-01", periods=6, freq="1min"),
            "system": ["srv"] * 6, "action": ["LOGIN_FAILED"] * 6,
            "ip_address": ["1.1.1.1"] * 6, "duration_sec": [0] * 6}
    df = pd.DataFrame(data)
    result = transform(df)
    assert result["fact_access"]["is_anomaly"].any()

def test_star_schema_tables_exist():
    """Transformação deve retornar as 4 tabelas do star schema"""
    df = pd.read_csv("data/raw/access_logs.csv")
    result = transform(df)
    assert set(result.keys()) == {"fact_access", "dim_user", "dim_system", "dim_time"}