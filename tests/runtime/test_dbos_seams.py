"""Compatibility checks for the deliberately quarantined DBOS internals."""

from __future__ import annotations


def test_private_dbos_config_processing_seam() -> None:
    from dbos._dbos_config import (
        process_config,
        translate_dbos_config_to_config_file,
    )

    processed = process_config(
        data=translate_dbos_config_to_config_file(
            {
                "name": "private-seam-test",
                "system_database_url": "sqlite:///:memory:",
                "enable_otlp": False,
                "otel_attribute_format": "semconv",
            }
        ),
        silent=True,
    )

    assert processed.get("name") == "private-seam-test"
    telemetry = processed.get("telemetry")
    assert telemetry is not None
    assert telemetry.get("disable_otlp") is True


def test_private_dbos_tracer_seam() -> None:
    from dbos._dbos_config import (
        process_config,
        translate_dbos_config_to_config_file,
    )
    from dbos._tracer import dbos_tracer

    processed = process_config(
        data=translate_dbos_config_to_config_file(
            {
                "name": "private-tracer-seam-test",
                "system_database_url": "sqlite:///:memory:",
                "enable_otlp": False,
                "otel_attribute_format": "semconv",
            }
        ),
        silent=True,
    )

    dbos_tracer.config(processed)
