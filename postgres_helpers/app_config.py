import logging
import platform
from os import environ
from pathlib import Path
from typing import (Union, Optional)

from dotenv import load_dotenv


def logging_config(log_file_name: Optional[str] = None,
                   force_local_folder: bool = False,
                   project_name: Optional[str] = None,
                   log_level: int = logging.DEBUG):
    """Create a basic logging file

    Args:
        log_file_name (Optional[str], optional): a file name ending with '.log' which will be stored in the log folder. Defaults to None.
        force_local_folder (bool=False): ignore system parameter and save logs locals within the downloads folder
        project_name (Optional[str]=None): names the logging folder, if ignored, uses the app name
        log_level (int=logging.DEBUG): the log level

    """
    if not project_name:
        project_name = get_project_root_path().name

    # Handles folder to log into
    if force_local_folder:
        logging_folder = Path(get_project_root_path(), project_name, "downloads")
        logging_folder.mkdir(parents=True, exist_ok=True)
    else:
        if platform.system() == 'Linux':
            logging_folder = Path('/var', "log", "my_apps", "python", project_name, )
            logging_folder.mkdir(parents=True, exist_ok=True)
        else:
            logging_folder = Path(get_project_root_path(), project_name, "downloads")
            logging_folder.mkdir(parents=True, exist_ok=True)
    # handles log file name
    if log_file_name:
        logging_file_path = Path(logging_folder, log_file_name)
    else:
        logging_file_path = Path(logging_folder, f'{project_name}.log')

    # Configure the root logger
    logging.basicConfig(
        filename=logging_file_path,  # Global log file name
        level=log_level,  # Global log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
            postgres_logins_in_environ() or load_dot_env_vars()
    ):
        raise Exception("No login details found to access Postgresql")
    else:
        return True


if __name__ == "__main__":
    print(get_project_root_path())

    print(load_postgres_details_to_env())
