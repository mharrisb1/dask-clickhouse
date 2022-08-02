from five18 import PyProjectToml

from dask_clickhouse import __version__


def test_version():
    pyproject_toml = PyProjectToml()
    assert __version__ == pyproject_toml.tool_table.poetry.version
