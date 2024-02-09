import json

import numpy as np
import pandas as pd


def json_schema_to_dataframe(schema_path, start_key=None):
    """Create an empty DataFrame based on a flattened JSON Schema, setting array columns to store JSON strings."""

    with open(schema_path, "r") as file:
        schema = json.load(file)

    def flatten_object(obj, parent_key="", sep="__"):
        items = {}
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                # Check if "bsonType" is either "object" directly or if it's a list containing "object"
                # if "$oid" in v:
                #     items[new_key] = ("object", None)
                # elif "$date" in v:
                #     items[new_key] = ("datetime64[ns]", "date")
                type = v.get("type", None)
                if not type:
                    raise ValueError(f"No type defined for ${new_key}")

                if type == "object":
                    items.update(flatten_object(v.get("properties", {}), new_key, sep=sep))
                elif type == "array":
                    # Handle arrays specifically, possibly by setting to JSON
                    items[new_key] = ("object", "json")
                else:
                    # Handle other types, possibly with multiple types in a list
                    dtype = map_json_type_to_dtype(type)
                    items[new_key] = (dtype, v.get("format"))
            else:
                items[new_key] = ("object", None)
        return items

    def map_json_type_to_dtype(json_type):
        print(json_type)
        knowntypes = {
            "string": "object",
            "number": "float",
            "integer": "int",
            "boolean": "bool",
            "object": "object",
            "array": "object",  # Arrays are treated as objects for JSON storage
            "objectId": "string",  # ObjectId as string
            "date": "datetime64[ns]",  # Date as datetime
        }
        if json_type not in knowntypes:
            raise ValueError(f"json type ${json_type} not in known types")
        return knowntypes.get(json_type, "object")

    if schema.get("type") == "object" and "properties" in schema:
        # Check if we should start flattening from a specific key
        if start_key and start_key in schema["properties"]:
            if schema["properties"][start_key]["type"] != "array":
                raise ValueError(f"Schema at start_key = '{start_key}' is not an array")
            schema_to_flatten = schema["properties"][start_key]["items"]["properties"]
        else:
            schema_to_flatten = schema["properties"]
        flattened_schema = flatten_object(schema_to_flatten)
    else:
        raise ValueError("Schema does not have a top-level object with properties")

    # Create an empty DataFrame with the specified dtypes
    columns = {k: pd.Series(dtype=np.dtype(v[0])) for k, v in flattened_schema.items()}
    df = pd.DataFrame(columns=columns)

    return df
