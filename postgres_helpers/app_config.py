from os import environ
from pathlib import Path
from typing import Union
import logging
from dotenv import load_dotenv
import platform


def logging_config():
    if platform.system() == "Linux":
        logging_folder = Path("/var", "log", "my_apps", "python", "postgres_helpers")
        logging_folder.mkdir(parents=True, exist_ok=True)
    else:
        logging_folder = Path(get_project_root_path(), "postgres_helpers", "downloads")
        logging_folder.mkdir(parents=True, exist_ok=True)

    logging_file_path = Path(logging_folder, "postgres_helpers.log")
    # Configure the root logger
    logging.basicConfig(
        filename=logging_file_path,  # Global log file name
        level=logging.DEBUG,  # Global log level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_project_root_path() -> Path:
    # https://stackoverflow.com/questions/5137497/find-the-current-directory-and-files-directory
    root_dir = Path(__file__).resolve().parent.parent
    # app_root_path: Path = Path().cwd().parent
    return root_dir


def load_dot_env_vars() -> bool:
    """Return True if Postgresql login details are loaded in environment"""
    env_path: Path = Path(get_project_root_path(), ".env")
    if not env_path:
        print(f"No .env file found for path: {env_path}")
        return False
    load_dotenv(env_path)

    # check that SQL logins details are in
    postgres_logins = [
        "POSTGRES_DB_HOST",
        "POSTGRES_DB_USER",
        "POSTGRES_DB_PASS",
        "POSTGRES_DB_NAME",
        "POSTGRES_DB_PORT",
    ]
    for postgres_login in postgres_logins:
        if environ.get(postgres_login) is None:
            print(f"Postgresql {postgres_login} login value not found in os.environ")
            return False

    return True


def load_config_secret_vars() -> bool:
    config_secret_path = Path(
        get_project_root_path(), "postgres_helpers", "app_config_secret.py"
    )
    if not config_secret_path.exists():
        print(f"No app_config_secret.py file found for path: {config_secret_path}")
        return False
    from postgres_helpers.app_config_secret import (
        POSTGRES_DB_HOST,
        POSTGRES_DB_USER,
        POSTGRES_DB_PASS,
        POSTGRES_DB_NAME,
        POSTGRES_DB_PORT,
    )

    environ["POSTGRES_DB_HOST"] = POSTGRES_DB_HOST
    environ["POSTGRES_DB_USER"] = POSTGRES_DB_USER
    environ["POSTGRES_DB_PASS"] = POSTGRES_DB_PASS
    environ["POSTGRES_DB_NAME"] = POSTGRES_DB_NAME
    environ["POSTGRES_DB_PORT"] = str(POSTGRES_DB_PORT)
    return True


def postgres_logins_in_environ() -> bool:
    """checks if logins details have been stored in environment"""
    postgres_logins = [
        "POSTGRES_DB_HOST",
        "POSTGRES_DB_USER",
        "POSTGRES_DB_PASS",
        "POSTGRES_DB_NAME",
        "POSTGRES_DB_PORT",
    ]
    return all(var in environ for var in postgres_logins)


def load_postgres_details_to_env() -> Union[None, bool]:
    """This will first try and find logins details in environ, then in .env file then in app_secret"""
    if not (
        postgres_logins_in_environ() or load_dot_env_vars() or load_config_secret_vars()
    ):
        raise Exception("No login details found to access Postgresql")
    else:
        return True


if __name__ == "__main__":
    # print(get_project_root_path())

    # load_postgres_details_to_env()
    load_config_secret_vars()
