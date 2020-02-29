import configparser
import click
import re

VERSION_FILE_PATH = "dagma/__version__.py"
TOML_FILE_PATH = "./pyproject.toml"

TOML_PROJECT_CONFIG_NAME = "tool.poetry"
TOML_VERSION_NAME = "version"


# === Helpers ===
def get_toml_version():
    """
    Get version number from pyproject.toml.
    """
    parser = configparser.ConfigParser()
    parser.read(TOML_FILE_PATH)

    return parser["tool.poetry"]["version"].strip('"')


def write_to_version_file(version):
    """
    Write new version number to projects __version__.py.
    """
    with open(VERSION_FILE_PATH, "w") as f_out:
        f_out.write(f"__version__ = {version}")


def write_to_toml(version):
    """
    Write new version number to pyproject.toml.
    """
    parser = configparser.ConfigParser()
    parser.read(TOML_FILE_PATH)

    parser[TOML_PROJECT_CONFIG_NAME][TOML_VERSION_NAME] = version

    with open(TOML_FILE_PATH, "w") as f_out:
        parser.write(f_out)


def write_version(version):
    version = f'"{version}"'
    write_to_version_file(version)
    write_to_toml(version)


# === CLI ===
VER_GET = "get"
VER_SET = "set"
VER_BUMP = "bump"

MAJOR_VERSION = "major"
MINOR_VERSION = "minor"
REV_VERSION = "rev"

VERSION_COMP_TO_POS = {
    MAJOR_VERSION: 0,
    MINOR_VERSION: 1,
    REV_VERSION: 2,
}


@click.group()
def cli():
    pass


@cli.command()
def get():
    """
    Get the current version number. Reads from the pyproject.toml file.
    """
    print(get_toml_version())


@cli.command()
@click.argument("new_version", type=click.STRING)
def set(new_version):
    """
    Set the version number (X.Y.Z). Must match X.Y.Z format (where X, Y, Z are numbers).
    """
    match = re.match(r"\d*\.\d*\.\d*", new_version)

    if not match:
        raise ValueError(f'Invalid version number "{new_version}".')

    write_version(new_version)


@cli.command()
@click.argument(
    "component", type=click.Choice([MAJOR_VERSION, MINOR_VERSION, REV_VERSION])
)
def bump(component):
    """
    Increase specified version component by one.
    """
    version = list(map(int, get_toml_version().split(".")))

    comp_ind = VERSION_COMP_TO_POS[component]

    version[comp_ind] += 1

    new_version = ".".join(map(str, version))

    write_version(new_version)


if __name__ == "__main__":
    cli()
