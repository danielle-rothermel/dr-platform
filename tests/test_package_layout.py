from importlib.util import find_spec


def test_functional_packages_are_discoverable() -> None:
    for package_name in (
        "dr_platform._core",
        "dr_platform.admission",
        "dr_platform.execution",
        "dr_platform.inspection",
        "dr_platform.pipeline",
        "dr_platform.recovery",
        "dr_platform.runtime",
        "dr_platform.submission",
    ):
        assert find_spec(package_name) is not None


def test_removed_internal_paths_have_no_compatibility_packages() -> None:
    for module_name in (
        "dr_platform.db",
        "dr_platform.dbos_config",
        "dr_platform.prefix",
        "dr_platform.staging",
        "dr_platform.telemetry",
    ):
        assert find_spec(module_name) is None
