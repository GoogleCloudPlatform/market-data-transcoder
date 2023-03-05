from setuptools import setup

if __name__ == "__main__":
    pkg_vars = {}
    with open("./transcoder/version.py") as fp:
        exec(fp.read(), pkg_vars)
    setup(
        version=pkg_vars['__version__']
    )
