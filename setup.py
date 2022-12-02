from setuptools import find_packages, setup

setup(
    name="postgres_helpers",
    packages=find_packages(),
    version="0.0.1",
    description="PostgresSQL Connection Helper in Async and Sync modes",
    author="Nono London",
    author_email="",
    license="Private",
    install_requires=["psycopg2",
                      "asyncpg",
                      "pandas"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    test_suite="tests",
)
