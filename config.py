import configparser


def read_config() -> dict:
    """Returns a Dictionary with the database configuration"""
    config = configparser.ConfigParser()

    config.read("./config/db.config.ini")

    user = config.get("DEFAULT", "USER")
    password = config.get("DEFAULT", "PASSWORD")
    dsn = config.get("DEFAULT", "DSN")
    min_ = config.get("DEFAULT", "MIN")
    max_ = config.get("DEFAULT", "MAX")
    inc = config.get("DEFAULT", "INCREMENT")

    db_config = {
        "user": user,
        "password": password,
        "dsn": dsn,
        "min": min_,
        "max": max_,
        "inc": inc,
    }

    return db_config
