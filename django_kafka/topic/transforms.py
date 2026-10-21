import datetime as dt


def days_from_epoch_to_date(days: int, default=None) -> dt.date | None:
    """
    DATE    INT32   io.debezium.time.Date
                    Represents the number of days since the epoch.
    [Temporal types](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types)
    """
    if days is None or days == "":
        return default
    return dt.date(1970, 1, 1) + dt.timedelta(days)
