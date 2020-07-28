import json
import os
import bplustree
import sys
from dataclasses import dataclass
from typing import List, Dict, Any
import json_func


import db_api
#from .db_api import DBField, SelectionCriteria

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[db_api.DBField]
    key_field_name: str

    def __init__(self, name, fields, key):
        self. name = name
        self.fields = fields
        self.key_field_name = key

    def count(self) -> int:
        return json_func.read_from_json("db_files/db.json")[self.name]["num_of_lines"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        data = json_func.read_from_json("db_files/db.json")
        primary_key = data[self.name]["key_field_name"]
        new_record = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}
        if data[self.name]["num_of_lines"] % 10 == 0 and data[self.name]["num_of_lines"] != 0:
            data[self.name]["num_of_files"] += 1
            json_func.write_to_json(f"db_files/{self.name}{data[self.name]['num_of_files']}.json", new_record)
        else:
            json_func.add_line_to_json(f"db_files/{self.name}{data[self.name]['num_of_files']}.json", values[primary_key] , new_record[values[primary_key]])
        data[self.name]["num_of_lines"] += 1
        json_func.write_to_json("db_files/db.json", data)


    def delete_record(self, key: Any) -> None:
        data = json_func.read_from_json("db_files/db.json")
        num_of_files = data[self.name]["num_of_files"]
        for file in range(num_of_files):
            json_func.delete_if_apear(f"db_files/{self.name}{file + 1}.json", key)
        data[self.name]["num_of_lines"] -= 1

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


def convert_from_dbfields(fields):
    fields_names = {}
    for field in fields:
        fields_names[f"{field.name}"] = field.type.__name__
    return fields_names


def str_to_class(field):
    return type(field)





def convert_to_db_fields(args):
    fields = []
    for k, v in args.itens():
        fields.append(db_api.DBField(k, str_to_class(v)))
    return fields


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    # Put here any instance information needed to support the API
    def __init__(self):
        json_func.write_to_json("db_files/db.json", {"num_of_tables": 0})

    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str, DB_BACKUP_ROOT=None) -> DBTable:
        d = {
            "fields": convert_from_dbfields(fields),
            "key_field_name": key_field_name,
            "num_of_lines": 0,
            "num_of_files": 1
        }
        json_func.write_to_json(f"db_files/{table_name}1.json", {})
        json_func.add_table_to_json("db_files/db.json", table_name, d)
        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        return json_func.read_from_json("db_files/db.json")["num_of_tables"]

    def get_table(self, table_name: str) -> DBTable:
        data = json_func.read_from_json("db_files/db.json")[table_name]
        return DBTable(table_name, convert_to_db_fields(data["fields"]), data["key_field_name"])

    def delete_table(self, table_name: str) -> None:
        num_of_files = json_func.delete_table_from_json(table_name)
        for file in range(num_of_files):
            path = f"db_files/{table_name}{file + 1}.json"
            if os.path.exist(path):
                os.remove(path)

    def get_tables_names(self) -> List[Any]:
        data = json_func.read_from_json("db_files/db.json")
        return [name for name in data.keys() if name != "num_of_tables"]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
