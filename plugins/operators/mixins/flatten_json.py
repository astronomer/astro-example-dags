import json
from datetime import datetime

import pandas as pd


class FlattenJsonDictMixin:
    def flatten_dict(self, d, parent_key="", separator="__"):
        """Recursively flatten nested dictionaries."""
        # print("flatten_dict called on ", parent_key, d)
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            if new_key in self.discard_fields:
                continue
            if isinstance(v, list):
                # Convert lists directly to JSON strings
                # print("Handling list", new_key, k, v)
                items[new_key] = json.dumps(v)
            elif isinstance(v, datetime):
                # print("Handling datetime", new_key, k, v)
                items[new_key] = pd.Timestamp(v)
            elif isinstance(v, dict):
                # print("Handling dict", new_key, k, v)
                items.update(
                    self.flatten_dict(
                        v,
                        parent_key=new_key,
                        separator=separator,
                    )
                )
            else:
                # print("Handling preserve", new_key, k, v)
                items[new_key] = v
        # print("items dict", items)
        return items

    def flatten_dataframe_columns_precisely(self, df):
        """Flatten all dictionary columns in a DataFrame and handle non-dict items."""
        flattened_data = pd.DataFrame()
        for column in df.columns:
            # Initialize a container for processed data
            column_data = []
            is_dict_column = df[column].apply(lambda x: isinstance(x, dict)).any()
            is_list_column = df[column].apply(lambda x: isinstance(x, list)).any()
            is_date_column = df[column].apply(lambda x: isinstance(x, datetime)).any()

            if is_date_column:
                # print("Handling datetime Top level column")
                column_df = df[column].apply(pd.Timestamp).to_frame(name=column)
            elif is_dict_column:
                for item in df[column]:
                    # Process dictionary items
                    if isinstance(item, dict):
                        # print("COLUMN item dict", column, item)
                        flattened_item = self.flatten_dict(
                            item,
                            separator=self.separator,
                        )
                        column_data.append(flattened_item)
                    else:
                        # For items that are not dicts (e.g., missing or null values), ensure compatibility
                        column_data.append({})
                # Normalize the processed column data
                column_df = pd.json_normalize(column_data)
                # Rename columns to ensure they are prefixed correctly
                column_df.columns = [
                    (f"{column}{self.separator}{subcol}" if not subcol == "PARENT_COLUMN" else column)
                    for subcol in column_df.columns
                ]

            elif is_list_column:
                # print("COLUMN item list", column)
                column_df = df[column].apply(json.dumps).to_frame(name=column)
            else:  # Directly append non-dict and non-list items
                # print("COLUMN item preserve", column)
                column_df = df[column].to_frame()

                # Concatenate the new column DataFrame to the flattened_data DataFrame
            flattened_data = pd.concat([flattened_data, column_df], axis=1)

        return flattened_data
