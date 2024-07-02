from enum import Enum
from typing import Any, Union, List
from pydantic import BaseModel


class WriteMode(Enum):
    APPEND = "append"                    # Append contents of this DataFrame to existing data.
    OVERWRITE = "overwrite"              # Overwrite existing data.
    ERROR_IF_EXISTS = "errorifexists"    # Throw an exception if data already exists.
    IGNORE = "ignore"                    # Silently ignore this operation if data already exists.


class Format(Enum):
    DELTA = "delta"
    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"



class Location(BaseModel):
    pass


class StorageLocation(Location):
    account_name: str
    container_name: str
    sub_path: str

    def _clean_sub_path(self) -> None:
        sub_path = self.sub_path
        if sub_path.startswith("/"):
           sub_path = sub_path[1:]
        if sub_path.endswith("/"):
            sub_path = sub_path[: -1]
        self.sub_path = sub_path

    def abfss_path(self) -> str:
        """Generate abfss URI."""
        self._clean_sub_path()
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{self.sub_path}"

    @staticmethod
    def from_url(url: str) -> Location:
        """Parse abfss URI."""
        url = url.replace("abfss://", "")
        container_name, account_n_path = url.split("@", 1)
        account_name = account_n_path.split(".", 1)[0]
        sub_path = account_n_path.split("/", 1)[1]
        return StorageLocation(
            account_name=account_name,
            container_name=container_name,
            sub_path=sub_path,
        )


class LocalLocation(Location):
    path: str


class Catalog(BaseModel):
    name: str
    storage_location: Location


class Database(BaseModel):
    name: str
    catalog: Catalog
    storage_location: Location


class Table(BaseModel):
    name: str
    table_schema: Any
    format: Format
    location: Location
    database: Database
    partition_by: Union[str, List[str], None] = None

    @property
    def full_location(self) -> str:
        """Generate full URL to the table's location."""
        return f"{self.location.abfss_path()}/{self.name}"

    @property
    def full_name(self) -> str:
        """Generate full name of the Table.

        Combines the catalog, schema and table name.
        """
        return f"`{self.database.catalog.name}`.`{self.database.name}`.`{self.name}`"


class Volume(BaseModel):
    name: str
    location: Location
    database: Database


class VolumeLocation(Location):
    volume: Volume

    @property
    def root_path(self) -> None:
        """Generate path for Databricks location."""
        name = self.volume.name
        catalog_name = self.volume.catalog.name
        database_name = self.volume.database.name
        return f"/Volumes/{catalog_name}/{database_name}/{name}"