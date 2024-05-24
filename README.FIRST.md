# Datalake

## Astro Docs are here

[Here](Astro.md)

## Running locally

```shell

astro config set -g disable_env_objects false
astro workspace list
# NAME         ID
# DataLake     clr9qwhbn033u01qzg6shab5j

astro dev start|restart --workspace-id <workspace_id>

```

## References

### .env

cp .env.example .env
Set your correct AIRFLOW_VAR_DEVICE_NAME

### Data Types used

Currently these are the data type mappings used to map from our schemas to NumPy datatypes, which are then mapped to SQL field types. <https://numpy.org/doc/stable/user/basics.types.html>

The subset we use are:-

``` python
bson_pandas_numpy_mapping = {
          "double": "Float64",  # BSON double maps to a 64-bit floating point
          "string": "string",  # BSON string maps to pandas object (for string data)
          "object": "object",  # BSON object maps to pandas object (for mixed types)
          "array": "object",  # BSON array maps to pandas object (for mixed types)
          "binData": "object",  # BSON binary data maps to pandas object (for arbitrary binary data)
          "objectId": "string",  # BSON ObjectId maps to pandas object (for unique object identifiers)
          "bool": "bool",  # BSON boolean maps to pandas/numpy boolean
          # "date": "datetime64[ns]",  # BSON date maps to pandas datetime64[ns]
          "int": "Int32",  # BSON 32-bit integer maps to pandas/numpy int32
          "timestamp": "datetime64[ns]",  # BSON timestamp maps to pandas datetime64[ns] (with note on precision)
          "long": "Int64",  # BSON 64-bit integer maps to pandas/numpy int64
          "decimal": "Float64",  # BSON Decimal128 maps to pandas/numpy float64 (considerations for precision apply)
      }
```
