# Function to generate the transformation logic for a single field
def _string_possibly_pounds_to_pence(field_name, *args):
    field = _convert_anything_to_something("int", field_name, *args)
    return field, ("Int32", None)


def _convert_pounds_to_pence_explicit(field_name):
    return {
        field_name: {
            "$toInt": {
                "$cond": {
                    "if": {"$eq": [{"$type": f"${field_name}"}, "double"]},
                    "then": {"$multiply": [f"${field_name}", 100]},
                    "else": f"${field_name}",
                }
            }
        }
    }, ("Int32", None)


def _convert_epoch(field_name):
    return {
        field_name: {
            "$cond": {
                "if": {
                    # If the value is greater than a certain threshold, it's already in milliseconds
                    "$gt": ["$" + field_name, 10000000000]
                },
                "then": {
                    # It's already in milliseconds, ensure it's a long integer
                    "$toLong": "$"
                    + field_name
                },
                "else": {
                    # Convert seconds to milliseconds by multiplying by 1000, and ensure it's a long integer
                    "$toLong": {"$multiply": [{"$toDouble": "$" + field_name}, 1000]}
                },
            }
        }
    }, ("Int64", None)


def _convert_anything_to_something(to_type, field_name):
    return {
        field_name: {
            "$cond": {
                "if": {"$in": [f"${field_name}", ["", " "]]},
                "then": None,
                "else": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": [{"$type": f"${field_name}"}, "string"]},  # Check if the field is a string
                                {
                                    "$regexMatch": {
                                        "input": f"${field_name}",
                                        "regex": r"^\d*\.\d+$",
                                    }
                                },
                            ]
                        },
                        "then": {
                            "$convert": {
                                "input": {
                                    "$multiply": [
                                        {
                                            "$convert": {
                                                "input": f"${field_name}",
                                                "to": "decimal",
                                                "onError": 0,
                                                "onNull": 0,
                                            }
                                        },
                                        100,
                                    ]
                                },
                                "to": f"{to_type}",
                                "onError": 0,
                                "onNull": None,
                            }
                        },
                        "else": {
                            "$convert": {
                                "input": f"${field_name}",
                                "to": f"{to_type}",
                                "onError": 0,
                                "onNull": None,
                            }
                        },
                    }
                },
            }
        }
    }


def convert_field(function_name, *function_args):
    if function_name not in _conversion_functions:
        raise ValueError(f"Unknown conversion function '{function_name}' given to convert()")

    return _conversion_functions[function_name](*function_args)


_conversion_functions = {
    "string_possibly_pounds_to_pence": _string_possibly_pounds_to_pence,
    "convert_epoch": _convert_epoch,
    "convert_pounds_to_pence_explicit": _convert_pounds_to_pence_explicit,
}
