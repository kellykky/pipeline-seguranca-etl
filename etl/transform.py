import pandas as pd


def transform(df: pd.DataFrame) -> dict:
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # --- Tabela FATO ---
    df["hour"] = df["timestamp"].dt.hour
    df["date"] = df["timestamp"].dt.date
    df["is_off_hours"] = df["hour"].apply(lambda h: h < 7 or h > 20)

    # Detecção de anomalia: >5 falhas do mesmo usuário em 10 minutos
    df = df.sort_values(["user", "timestamp"]).reset_index(drop=True)

    # Criamos uma coluna numérica (1 para falha, 0 para sucesso)
    df["is_failure"] = (~df["success"]).astype(int)

    # Calculamos a soma móvel usando a janela de 10 minutos
    df["failed_attempts_10min"] = (
        df.set_index("timestamp")
        .groupby("user")["is_failure"]
        .rolling("10min")
        .sum()
        .reset_index(level=0, drop=True)
        .values  # .values previne qualquer erro de alinhamento de index
    )

    df["is_anomaly"] = df["failed_attempts_10min"] >= 5
    df = df.drop(columns=["is_failure"])  # Limpa a coluna auxiliar
    fact_table = df[[
        "timestamp", "user", "system", "action",
        "ip_address", "success", "duration_sec",
        "is_off_hours", "is_anomaly"
    ]]

    # --- Tabelas DIMENSÃO (star schema) ---
    dim_user = df[["user"]].drop_duplicates().reset_index(drop=True)
    dim_user["user_id"] = dim_user.index + 1
    dim_user["is_privileged"] = dim_user["user"].isin(
        ["admin", "root", "svc_backup"])

    dim_system = df[["system"]].drop_duplicates().reset_index(drop=True)
    dim_system["system_id"] = dim_system.index + 1

    dim_time = pd.DataFrame({
        "date": df["date"].unique()
    })
    dim_time["week"] = pd.to_datetime(dim_time["date"]).dt.isocalendar().week
    dim_time["month"] = pd.to_datetime(dim_time["date"]).dt.month

    return {
        "fact_access": fact_table,
        "dim_user": dim_user,
        "dim_system": dim_system,
        "dim_time": dim_time
    }
