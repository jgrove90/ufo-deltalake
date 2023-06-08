#from importlib_metadata import PackageNotFoundError
from importlib.metadata import PackageNotFoundError
import pytest
from app_utils import app_utils

@pytest.mark.parametrize("package_name", ["pytest", "unknown-package"])
def test_get_package_version(package_name):
    """Test the get_package_version function."""

    # Check that the function returns the correct version for a known package.
    if package_name == "pytest":
        version = "7.3.1"
        assert app_utils.get_package_version(package_name) == version
    else:
        with pytest.raises(PackageNotFoundError):
            app_utils.get_package_version(package_name)