"""PostgreSQL proofs for the DBOS mechanisms used by later rebuild stages."""

from __future__ import annotations

from uuid import uuid4

from dbos import DBOS, DBOSClient, DBOSConfig, EnqueueOptions, SetWorkflowID
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

PLATFORM_ROW_ID = 1


def _create_platform_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE dbos_spike_platform_rows (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO dbos_spike_platform_rows (id, value)
                VALUES (:id, 'initial')
                """
            ),
            {"id": PLATFORM_ROW_ID},
        )


def _launch_dbos(database_url: str, *, application_database: bool) -> None:
    config: DBOSConfig = {
        "name": "dr-platform-dbos-spike",
        "system_database_url": database_url,
        "application_version": "dbos-spike-v1",
        "run_admin_server": False,
        "use_listen_notify": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    if application_database:
        config["application_database_url"] = database_url
    DBOS(config=config)
    DBOS.launch()


def test_enqueue_in_platform_transaction_commits_and_rolls_back_atomically(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    _create_platform_table(pg_engine)
    try:
        _launch_dbos(clean_pg, application_database=False)
    finally:
        DBOS.destroy(destroy_registry=True)

    client = DBOSClient(system_database_url=clean_pg)
    invocation_id = uuid4().hex
    commit_workflow_id = f"dbos-spike-committed-{invocation_id}"
    rollback_workflow_id = f"dbos-spike-rolled-back-{invocation_id}"
    options: EnqueueOptions = {
        "workflow_name": "dbos_spike_workflow",
        "queue_name": "dbos_spike_queue",
        "workflow_id": commit_workflow_id,
    }

    try:
        with Session(pg_engine) as session, session.begin():
            session.execute(
                text(
                    """
                    UPDATE dbos_spike_platform_rows
                    SET value = 'committed'
                    WHERE id = :id
                    """
                ),
                {"id": PLATFORM_ROW_ID},
            )
            handle = client.enqueue_in_transaction(session, options, "payload")
            assert handle.get_workflow_id() == commit_workflow_id

        with pg_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT value FROM dbos_spike_platform_rows "
                        "WHERE id = :id"
                    ),
                    {"id": PLATFORM_ROW_ID},
                ).scalar_one()
                == "committed"
            )
            assert connection.execute(
                text(
                    """
                    SELECT status, queue_name
                    FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": commit_workflow_id},
            ).one() == ("ENQUEUED", "dbos_spike_queue")

        options["workflow_id"] = rollback_workflow_id
        with Session(pg_engine) as session:
            transaction = session.begin()
            session.execute(
                text(
                    """
                    UPDATE dbos_spike_platform_rows
                    SET value = 'rolled back'
                    WHERE id = :id
                    """
                ),
                {"id": PLATFORM_ROW_ID},
            )
            client.enqueue_in_transaction(session, options, "payload")
            transaction.rollback()

        with pg_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT value FROM dbos_spike_platform_rows"
                        "WHERE id = :id"
                    ),
                    {"id": PLATFORM_ROW_ID},
                ).scalar_one()
                == "committed"
            )
            assert (
                connection.execute(
                    text(
                        """
                    SELECT count(*)
                    FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                    ),
                    {"workflow_id": rollback_workflow_id},
                ).scalar_one()
                == 0
            )
    finally:
        client.destroy()


def test_dbos_checkpointed_transaction_writes_platform_table(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    _create_platform_table(pg_engine)

    @DBOS.transaction(name="dbos_spike_write_platform_row")
    def write_platform_row(value: str) -> str:
        DBOS.sql_session.execute(
            text(
                """
                UPDATE dbos_spike_platform_rows
                SET value = :value
                WHERE id = :id
                """
            ),
            {"id": PLATFORM_ROW_ID, "value": value},
        )
        return value

    @DBOS.workflow(name="dbos_spike_checkpointed_workflow")
    def checkpointed_workflow(value: str) -> str:
        return write_platform_row(value)

    workflow_id = f"dbos-spike-checkpointed-transaction-{uuid4().hex}"
    try:
        _launch_dbos(clean_pg, application_database=True)
        with SetWorkflowID(workflow_id):
            assert checkpointed_workflow("checkpointed") == "checkpointed"
    finally:
        DBOS.destroy(destroy_registry=True)

    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT value FROM dbos_spike_platform_rowsWHERE id = :id"
                ),
                {"id": PLATFORM_ROW_ID},
            ).scalar_one()
            == "checkpointed"
        )
        assert (
            connection.execute(
                text(
                    """
                SELECT function_name
                FROM dbos.transaction_outputs
                WHERE workflow_uuid = :workflow_id
                """
                ),
                {"workflow_id": workflow_id},
            ).scalar_one()
            == "dbos_spike_write_platform_row"
        )
