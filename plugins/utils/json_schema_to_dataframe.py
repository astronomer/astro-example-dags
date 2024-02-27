import json


def flatten_object(obj, parent_key="", sep="__", discard_fields=[]):
    items = {}
    print("FLATTEN_OBJECT", obj)
    for k, v in obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if new_key in discard_fields:
            print(f"Skipping discarded field ${new_key}")
            continue
        # print(f"Flattenning {new_key}")
        if isinstance(v, dict):
            # Check if "bsonType" is either "object" directly or if it's a list containing "object"
            # if "$oid" in v:
            #     items[new_key] = ("object", None)
            # elif "$date" in v:
            #     items[new_key] = ("datetime64[ns]", "date")
            ts_type = v.get("tsType", None)  # Extract tsType if available
            vtype = v.get("type", None)
            vformat = v.get("format")
            if not vtype:
                raise ValueError(f"No type defined for ${new_key}")

            if vtype == "object":
                items.update(
                    flatten_object(
                        v.get("properties", {}),
                        new_key,
                        sep=sep,
                        discard_fields=discard_fields,
                    )
                )
            elif vtype == "array":
                # Handle arrays specifically, possibly by setting to JSON
                items[new_key] = ("object", "json")
            else:
                # Handle other types, possibly with multiple types in a list
                dtype = map_json_type_to_dtype(vtype, ts_type)
                items[new_key] = (dtype, vformat)
                print(f"TYPE MAPPING {new_key}=>{dtype}, {vformat}")
        else:
            raise ValueError(f"${new_key} ${v} is not a dict")
            # dtype = map_json_type_to_dtype(vtype, ts_type)
            # items[new_key] = (dtype, vformat)
            # print(f"TYPE MAPPING {new_key}=>{dtype}, {vformat}")
    return items


def map_json_type_to_dtype(json_type, ts_type=None):
    # Updated known types mapping to use pd.StringDtype() for strings
    knowntypes = {
        "string": "string",  # Use pandas' nullable string type
        "number": "float",
        "integer": "Int32",
        "boolean": "bool",
        "object": "object",  # Keep as is or adjust based on specific needs
        "array": "object",  # Arrays are treated as objects for JSON storage
        "objectId": "string",  # Assuming objectId is also a string
    }
    # Adjust date handling based on tsType
    if json_type == "date":
        if ts_type == "Date" or ts_type == "Date | moment":
            return "datetime64[ns, UTC]"  # or "datetime64[ns]" for naive datetime
        else:
            return "string"

    return knowntypes.get(json_type, "string")


def json_schema_to_dataframe(schema_path, start_key=None, discard_fields=[]):
    """Create an empty DataFrame based on a flattened JSON Schema, setting array columns to store JSON strings."""

    with open(schema_path, "r") as file:
        schema = json.load(file)

    if schema.get("type") == "object" and "properties" in schema:
        # Check if we should start flattening from a specific key
        if start_key and start_key in schema["properties"]:
            print(schema.get("properties")[start_key])
            if schema["properties"][start_key]["type"] != "array":
                raise ValueError(f"Schema at start_key = '${start_key}' is not an array")
            if "properties" in schema["properties"][start_key]["items"]:
                # we have an array of objects
                schema_to_flatten = schema["properties"][start_key]["items"]["properties"]
            else:
                # The schema suggests we will have an array of values
                # However the actual aggregation should be converting this into an array of objects
                schema_to_flatten = {"id": schema["properties"][start_key]["items"]}
        else:
            schema_to_flatten = schema["properties"]
        flattened_schema = flatten_object(schema_to_flatten, discard_fields=discard_fields)
    else:
        raise ValueError("Schema does not have a top-level object with properties")

    print(flattened_schema.items())
    # Create an empty DataFrame with the specified dtypes
    # column_dtypes = {k: v[0] for k, v in flattened_schema.items()}

    return flattened_schema
