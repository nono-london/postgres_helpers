from setuptools import find_packages, setup
import platform
setup(
    name="postgres_helpers",
    packages=find_packages(),
    version="0.0.3.1",
    description="PostgresSQL Connection Helper in Async and Sync modes",
    author="Nono London",
    author_email="",
    url="https://github.com/nono-london/postgres_helpers",
    license="MIT",
    install_requires=["asyncpg",
                      "python-dotenv",
                      "pandas"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    test_suite="tests",
)

if platform.system() != 'Linux':
    setup(
        install_requires=["psycopg2"]
    )
