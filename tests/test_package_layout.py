from importlib.util import find_spec


def test_functional_packages_are_discoverable() -> None:
    for package_name in (
        "dr_platform._core",
        "dr_platform.admission",
        "dr_platform.completion",
        "dr_platform.execution",
        "dr_platform.inspection",
        "dr_platform.pipeline",
        "dr_platform.recovery",
        "dr_platform.runtime",
        "dr_platform.submission",
    ):
        assert find_spec(package_name) is not None
