import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="postgres_helpers",
    version="0.0.3.6",
    author="Nono London",
    author_email="",
    description="PostgresSQL Connection Helper in Async, Sync and Pool modes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nono-london/postgres_helpers",
    packages=["postgres_helpers"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["asyncpg", "python-dotenv", "pandas"],
    tests_require=["pytest"],
    python_requires='>=3.9',
)
