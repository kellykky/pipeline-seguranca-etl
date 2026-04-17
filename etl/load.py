from sqlalchemy import create_engine
import os


def load_to_postgres(tables: dict):
    engine = create_engine(
        "postgresql+psycopg2://admin:admin@postgres:5432/security_logs"
    )

    for table_name, df in tables.items():
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )
        print(f"Tabela '{table_name}' carregada com {len(df)} linhas")
