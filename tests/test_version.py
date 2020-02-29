import configparser

from dagma import __version__


def get_version():
    parser = configparser.ConfigParser()
    parser.read("pyproject.toml")
    return parser["tool.poetry"]["version"].strip('"')


def test_version():
    assert __version__ == get_version()
