import os
import re
import hashlib

# Define regex pattern to match different SQL types
pattern = r"""
CREATE\s+MATERIALIZED VIEW\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+OR\s+REPLACE\s+VIEW\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+{{\s*schema\s*}}\.(\w+)
"""


def get_recursive_sql_file_lists(directory, first_call=True, subdir="reports", add_table_columns_to_context=[]):
    grouped_file_info = []
    print("Current Working Directory:", os.getcwd())

    # Extract the last directory name from the root directory path
    last_dir_name = os.path.basename(os.path.normpath(directory))

    current_level_files = []
    current_level_entities = []  # List to store filenames at the current level
    # Process .sql files directly in the given directory
    if first_call or os.path.basename(directory) == subdir:
        for item in os.listdir(directory):
            full_path = os.path.join(directory, item)
            if os.path.isfile(full_path) and item.endswith(".sql"):
                with open(full_path, "rb") as f:
                    content = f.read()
                    sha256_hash = hashlib.sha256(content).hexdigest()

                filename_without_extension, _ = os.path.splitext(item)
                # Add filename to the current level filenames list
                # Construct the modified filepath to only include the last directory and onwards
                path_parts = full_path.split(os.sep)
                last_dir_index = path_parts.index(last_dir_name)
                modified_filepath = os.sep.join(path_parts[last_dir_index:])
                sql_string = content.decode("utf-8")
                # Extract entity names from the SQL string using the pattern
                matches = re.findall(pattern, sql_string, re.IGNORECASE)
                for match in matches:
                    entity_name = next(filter(None, match[1:]), None)  # Filter out empty matches and get the name
                    if entity_name:
                        print(f"Matched {entity_name}")
                        current_level_entities.append(entity_name)
                    else:
                        print(f"Failed to match for {filename_without_extension}")

                file_info = {
                    "filename": filename_without_extension,
                    "filepath": modified_filepath,
                    "checksum": sha256_hash,
                    "sql": sql_string,
                    "add_table_columns_to_context": add_table_columns_to_context.copy(),  # Add previous filenames
                }
                current_level_files.append(file_info)

    if current_level_files:
        grouped_file_info.append(current_level_files)

    # Cumulatively pass the filenames
    cumulative_entity_names = add_table_columns_to_context + current_level_entities

    # Recursively process subdirectories, focusing on "subreports" if not the first call
    for item in os.listdir(directory):
        full_path = os.path.join(directory, item)
        if os.path.isdir(full_path):
            # Process all directories on the first call; afterwards, only "subreports"
            if first_call or item == subdir:
                subdirectory_files = get_recursive_sql_file_lists(
                    full_path,
                    first_call=False,
                    subdir=subdir,
                    add_table_columns_to_context=cumulative_entity_names,
                )
                if subdirectory_files:
                    grouped_file_info.extend(subdirectory_files)

    return grouped_file_info
