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

    def abfss_path(self):
        self._clean_sub_path()
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{self.sub_path}"


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
    catalog: Catalog
    database: Database
    partition_by: Union[str, List[str], None] = None

    @property
    def full_location(self) -> str:
        return f"{self.location.abfss_path()}/{self.name}"
    
    @property
    def full_name(self) -> str:
        return f"`{self.catalog.name}`.`{self.database.name}`.`{self.name}`"
    

class Volume(BaseModel):
    name: str
    location: Location
    catalog: Catalog
    database: Database

    @property
    def full_location(self) -> str:
        return f"{self.location.abfss_path()}/{self.name}"

    @property
    def full_name(self) -> str:
        return f"`{self.catalog.name}`.`{self.database.name}`.`{self.name}`"