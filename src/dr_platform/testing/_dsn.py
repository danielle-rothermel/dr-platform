from __future__ import annotations

from sqlalchemy import make_url


def validate_test_database_url(database_url: str) -> None:
    """Reject DSNs that are unsafe for automated test migration."""
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError("DR_PLATFORM_TEST_DATABASE_URL must use PostgreSQL")
    database_identity_overrides = {
        key.lower()
        for key in url.query
        if key.lower() in {"dbname", "service", "servicefile"}
    }
    if database_identity_overrides:
        raise ValueError(
            "DR_PLATFORM_TEST_DATABASE_URL must not override database "
            "identity through query parameters"
        )
    database_name = url.database
    if database_name is None or not database_name.endswith("_test"):
        raise ValueError(
            "DR_PLATFORM_TEST_DATABASE_URL must name a database ending in "
            "'_test'"
        )
