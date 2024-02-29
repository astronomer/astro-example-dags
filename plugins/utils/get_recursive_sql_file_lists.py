import os
import hashlib


def get_recursive_sql_file_lists(directory, first_call=True, subdir="subreports"):
    grouped_file_info = []
    print("Current Working Directory:", os.getcwd())

    # Extract the last directory name from the root directory path
    last_dir_name = os.path.basename(os.path.normpath(directory))

    current_level_files = []
    # Process .sql files directly in the given directory
    if first_call or os.path.basename(directory) == subdir:
        for item in os.listdir(directory):
            full_path = os.path.join(directory, item)
            if os.path.isfile(full_path) and item.endswith(".sql"):
                with open(full_path, "rb") as f:
                    content = f.read()
                    sha256_hash = hashlib.sha256(content).hexdigest()

                filename_without_extension, _ = os.path.splitext(item)
                # Construct the modified filepath to only include the last directory and onwards
                path_parts = full_path.split(os.sep)
                last_dir_index = path_parts.index(last_dir_name)
                modified_filepath = os.sep.join(path_parts[last_dir_index:])

                file_info = {
                    "id": filename_without_extension,
                    "filepath": modified_filepath,
                    "hash": sha256_hash,
                }
                current_level_files.append(file_info)

    if current_level_files:
        grouped_file_info.append(current_level_files)

    # Recursively process subdirectories, focusing on "subreports" if not the first call
    for item in os.listdir(directory):
        full_path = os.path.join(directory, item)
        if os.path.isdir(full_path):
            # Process all directories on the first call; afterwards, only "subreports"
            if first_call or item == subdir:
                subdirectory_files = get_recursive_sql_file_lists(full_path, first_call=False, subdir=subdir)
                if subdirectory_files:
                    grouped_file_info.extend(subdirectory_files)

    return grouped_file_info
