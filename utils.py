import oracledb
from sqlalchemy import create_engine

def remove_after_note(text):
    index = text.find("Note")
    if index != -1:
        return text[:index]
    return text
def remove_after_conf(text):
    index = text.find("Confidence")
    if index != -1:
        return text[:index]
    return text

def db_Connect_thinModePool(user, password, dsn, min=1, max=5, increment=1):
    try:
        ConnectionPool = oracledb.create_pool(
            user=user, password=password, dsn=dsn, min=min, max=max, increment=increment)
        engine = create_engine('oracle+oracledb://',
                               creator=ConnectionPool.acquire)
        return engine
    except Exception as e:
        print(f"DB Error:  {e}")
        return None
