"""Bootstrap smoke test: the package imports and exposes its version."""

import fc


def test_package_imports() -> None:
    """The top-level ``fc`` package imports and carries a version string."""
    assert isinstance(fc.__version__, str)
    assert fc.__version__
