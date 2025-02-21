from setuptools import setup

setup(
    name="yandex_metrika_logs",
    version="0.1.0",
    description="Yandex Metrika Logs API Downloader",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Nick Toverovskiy",
    url="https://github.com/mmmspace/metrika",
    py_modules=['yandex_metrika_logs_api'],  # Use py_modules for single file
    install_requires=[],  # no external dependencies currently
    python_requires=">=3.6",
)
