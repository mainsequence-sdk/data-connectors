import os
import os
try:
    import tomllib as _toml  # Py 3.11+
except ModuleNotFoundError:
    try:
        import tomli as _toml  # Py 3.10-
    except ModuleNotFoundError:
        import toml as _toml   # final fallback to old 'toml' package

from setuptools import setup, find_packages
import subprocess
from setuptools.command.install import install

# Which Pipfile sections should NOT become extras
# (We handle 'packages' as the base install_requires,
#  and generally skip 'dev-packages', 'requires', 'source'.)
EXCLUDE_SECTIONS = {"source", "packages", "dev-packages", "requires", "scripts"}

class PostInstallCommand(install):
    """
    Custom 'install' command that runs additional shell commands
    after the standard install.
    """
    def run(self):
        # Run the standard install process first
        install.run(self)

def parse_package_dict(package_dict):
    """
    Convert a single Pipfile section (e.g. [packages], [prices-alpaca]) into
    a list of requirement strings.
    """
    reqs = []
    for pkg, spec in package_dict.items():
        # For a spec like '*' or a string version
        if isinstance(spec, str):
            if spec == "*":
                reqs.append(pkg)
            else:
                # E.g., pkg="some-lib", spec="==1.2.3"
                reqs.append(f"{pkg}{spec}")
        elif isinstance(spec, dict):
            # If there's a local file reference, skip or handle it
            if "file" in spec:
                # For example, skip local references or handle them as needed
                continue
            # Check for extras, e.g. {version="==1.2.3", extras=["some-extra"]}
            if "extras" in spec:
                extras_list = ",".join(spec["extras"])
                version_part = spec.get("version", "")  # e.g. "==1.2.3"
                # Example: 'ray[default]==1.2.3'
                reqs.append(f"{pkg}[{extras_list}]{version_part}")
            else:
                # Just a version or blank
                version_part = spec.get("version", "")
                if version_part:
                    reqs.append(f"{pkg}{version_part}")
                else:
                    reqs.append(pkg)
        else:
            # If spec is None or some unexpected type, skip or handle
            continue
    return reqs

def get_install_requirements(pipfile_toml):
    """
    Parse the `[packages]` section for base (always-installed) dependencies.
    """
    if "packages" not in pipfile_toml:
        return []
    return parse_package_dict(pipfile_toml["packages"])

def get_extras_requirements(pipfile_toml):
    """
    Convert each custom section (like [prices-alpaca]) into an extra_require entry.
    Example result: {"prices-alpaca": ["alpaca-py"]}
    """
    extras = {}
    for section_name, package_dict in pipfile_toml.items():
        # Skip sections we don't want as extras
        if section_name in EXCLUDE_SECTIONS:
            continue
        # package_dict should be a dict of dependencies
        if not isinstance(package_dict, dict):
            continue
        # Parse them into a list of requirements
        extras[section_name] = parse_package_dict(package_dict)

    return extras

def load_pipfile():
    """
    Loads the Pipfile (as TOML) if it exists.
    """
    pipfile_path = os.path.join(os.path.dirname(__file__), "Pipfile")
    if not os.path.isfile(pipfile_path):
        return {}
    with open(pipfile_path, 'r') as fh:
        return _toml.loads(fh.read())

pipfile_toml = load_pipfile()

setup(
    name='data_connectors',
    version='2.0.0',
    python_requires='>=3.9.0',
    author='Main Sequence GmbH',
    author_email='',
    packages=find_packages(include=['data_connectors', 'data_connectors.*']),

    # Base requirements from [packages]
    install_requires=get_install_requirements(pipfile_toml),
    cmdclass={
            # Use our custom install command
            'install': PostInstallCommand
        },

    # Extras from other sections (like [prices-alpaca])
    extras_require=get_extras_requirements(pipfile_toml),
)
