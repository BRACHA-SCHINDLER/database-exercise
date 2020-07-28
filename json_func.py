import json
from os import close


def write_to_json(path, data):
    with open(path, "w") as the_file:
        json.dump(data, the_file)


def add_table_to_json(path, table_name,  data):
    the_file = open(path)
    json_data = json.load(the_file)
    json_data[table_name] = data
    json_data["num_of_tables"] += 1
    write_to_json(path, json_data)


def add_line_to_json(path, primary_key,  data):
    the_file = open(path)
    json_data = json.load(the_file)
    json_data[primary_key] = data
    write_to_json(path, json_data)


def read_from_json(path):
    the_file = open(path)
    data = json.load(the_file)
    the_file.close()
    return data


def delete_table_from_json(table_name):
    the_file = open("db_files/db.json")
    json_data = json.load(the_file)
    num_of_files = json_data[table_name]["num_of_files"]
    del json_data[table_name]
    write_to_json("db_files/db.json", json_data)
    return num_of_files


def delete_if_apear(path, key):
    the_file = open("db_files/db.json")
    json_data = json.load(the_file)
    for k, v in json_data.items():
        if k == key:
            del json_data[k]
        else:
            for value in v.values():
                if value == key:
                    del json_data[k]