from setuptools import setup, find_packages

setup(
    name="yandex-metrika-logs",
    version="0.1.0",
    description="Yandex Metrika Logs API Downloader",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Nick Toverovskiy",
    url="https://github.com/mmmspace/metrika",
    packages=find_packages(),
    install_requires=[],  # no external dependencies currently
    python_requires=">=3.6",
) 