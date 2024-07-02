from enum import Enum
from dotenv import load_dotenv

from pydantic_settings import BaseSettings, SettingsConfigDict


load_dotenv()


class Environment(Enum):
    """An enumeration representing different environments."""

    LOCAL = "LOCAL"
    DEV = "DEV"
    TEST = "TEST"
    STAGE = "STAGE"
    PROD = "PROD"


class LogLevel(Enum):
    """Enum representing different log levels."""

    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class BaseConfiguration(BaseSettings):
    """Base configuration object based on pydantic.BaseSettings."""

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
    )


class StorageConfiguration(BaseSettings):
    """Base configuration of dedicated storage account to Unity Catalog."""

    account_name: str = ""
    container_name: str = ""


class MainConfiguration(BaseConfiguration):
    """Configuration class for the service."""

    unity_storage: StorageConfiguration = StorageConfiguration()
    external_storage: StorageConfiguration = StorageConfiguration()
    ingress_storage: StorageConfiguration = StorageConfiguration()

configuration: MainConfiguration = MainConfiguration()
