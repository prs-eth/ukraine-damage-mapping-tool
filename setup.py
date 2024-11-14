from setuptools import find_packages, setup

setup(
    name="src",
    version="1.0.0",
    author="Olivier Dietrich",
    author_email="odietrich@ethz.ch",
    description="Detecting building destruction in Ukraine using pixel-wise Sentine-1 amplitude time series",
    url="https://github.com/olidietrich/ukraine-damage-mapping-tool",
    packages=find_packages(),
    test_suite="src.tests.test_all.suite",
)
