import oracledb
from sqlalchemy import (
    create_engine,
    inspect,
)


def db_Connect_thinModePool(config) -> dict:
    """Connect to Oracle Database using the thin mode pool"""
    table_names = set()
    try:
        oracledb.defaults.stmtcachesize = 40
        oracledb.init_oracle_client()
        connectionPool = oracledb.create_pool(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"],
            min=config["min"],
            max=config["max"],
            increment=config["inc"],
            homogeneous=False,
            stmtcachesize=50,
        )
        engine = create_engine(
            "oracle+oracledb://", creator=connectionPool.acquire, pool_pre_ping=True
        )
        connection = engine.connect()
        inspector = inspect(engine)
        for table_name in inspector.get_table_names(schema="TEST_SCHEMA"):
            table_names.add(table_name)
        return {"connection": connection, "engine": engine, "table_names": table_names}
    except Exception as e:
        print(f"DB Error:  {e}")
        return {}
