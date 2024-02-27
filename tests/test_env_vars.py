from postgres_helpers.app_config import load_postgres_details_to_env
from postgres_helpers.app_config import logging_config

logging_config()


def test_postgres_in_env():
    assert load_postgres_details_to_env()


if __name__ == '__main__':
    test_postgres_in_env()
