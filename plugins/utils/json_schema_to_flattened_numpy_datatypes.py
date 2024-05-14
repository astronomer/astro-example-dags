import json


def flatten_object(obj, parent_key="", sep="__", discard_fields=[], preserve_fields={}):
    items = preserve_fields.copy()
    print("FLATTEN_OBJECT", obj)
    for k, v in obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if new_key in discard_fields:
            print(f"Skipping discarded field ${new_key}")
            continue
        # print(f"Flattenning {new_key}")
        if isinstance(v, dict):
            ts_type = v.get("tsType", None)  # Extract tsType if available
            comment = v.get("$comment", None)  # Extract $comment if available
            vtype = v.get("type", None)
            btype = v.get("bsonType", None)
            if not ts_type:
                if k in ["createdAt", "updatedAt"]:
                    ts_type = "Date"

            if isinstance(btype, list):
                for btype_string in btype:
                    # find first non null btype in the array
                    if btype_string != "null":
                        btype = btype_string
                        break

            vformat = v.get("format")
            if not vtype:
                raise ValueError(f"No type defined for ${new_key}")

            if not btype:
                raise ValueError(f"No bsonType defined for ${new_key}")

            print(f"TYPES for Key {new_key}=>{btype}, {vtype}, {ts_type}")
            if btype == "object":
                items.update(
                    flatten_object(
                        v.get("properties", {}),
                        new_key,
                        sep=sep,
                        discard_fields=discard_fields,
                    )
                )
            elif btype == "array":
                # Handle arrays specifically, possibly by setting to JSON
                items[new_key] = ("object", "json")
            else:
                # Handle other types, possibly with multiple types in a list
                dtype = map_bson_type_to_dtype(btype, ts_type, comment)
                items[new_key] = (dtype, vformat)
                print(f"bsonType MAPPING {new_key}=>{dtype}, {vformat} {ts_type}, for {btype}")
        else:
            raise ValueError(f"${new_key} ${v} is not a dict")
    return items


def map_bson_type_to_dtype(bson_type, ts_type=None, comment=None):
    # Adjust date handling based on tsType
    if bson_type == "date":
        if ts_type == "Date" or ts_type == "Date | moment":
            if comment and comment == "naive":
                return "datetime64[ns]"
            return "datetime64[ns, UTC]"
        else:
            return "string"

    bson_pandas_numpy_mapping = {
        "double": "Float64",  # BSON double maps to a 64-bit floating point
        "string": "string",  # BSON string maps to pandas object (for string data)
        "object": "object",  # BSON object maps to pandas object (for mixed types)
        "array": "object",  # BSON array maps to pandas object (for mixed types)
        "binData": "object",  # BSON binary data maps to pandas object (for arbitrary binary data)
        "objectId": "string",  # BSON ObjectId maps to pandas object (for unique object identifiers)
        "bool": "Int32",  # BSON boolean maps to pandas/numpy boolean
        # "date": "datetime64[ns]",  # BSON date maps to pandas datetime64[ns]
        "int": "Int32",  # BSON 32-bit integer maps to pandas/numpy int32
        "timestamp": "datetime64[ns]",  # BSON timestamp maps to pandas datetime64[ns] (with note on precision)
        "long": "Int64",  # BSON 64-bit integer maps to pandas/numpy int64
        "decimal": "Float64",  # BSON Decimal128 maps to pandas/numpy float64 (considerations for precision apply)
    }

    return bson_pandas_numpy_mapping.get(bson_type, "string")


def json_schema_to_flattened_numpy_datatypes(schema_path, start_key=None, discard_fields=[], preserve_fields={}):
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
                # So it should tell us what this schema looks like in the preserve_fields dict
                print(f"Returning the preserve_fields array for {start_key}")
                return preserve_fields
                # schema_to_flatten = {"id": schema["properties"][start_key]["items"]}
        else:
            schema_to_flatten = schema["properties"]
        flattened_schema = flatten_object(
            schema_to_flatten,
            discard_fields=discard_fields,
            preserve_fields=preserve_fields,
        )
    else:
        raise ValueError("Schema does not have a top-level object with properties")

    print(flattened_schema.items())

    return flattened_schema
