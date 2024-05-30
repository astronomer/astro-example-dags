import re

# Define regex pattern to match different SQL types
pattern = re.compile(
    r"""
\s+CREATE\s+MATERIALIZED\s+VIEW\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+VIEW\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+OR\s+REPLACE\s+VIEW\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+TABLE\s+{{\s*schema\s*}}\.(\w+)\s*|
\s+CREATE\s+OR\s+REPLACE\s+FUNCTION\s+{{\s*schema\s*}}\.(\w+)
""",
    re.IGNORECASE | re.VERBOSE,
)


def extract_entity_name(sql_string):
    matches = re.findall(pattern, sql_string)
    # print("PATTERN", pattern)
    # print("SQL_STRING", sql_string)
    # print("MATCHES", matches, len(matches))
    if len(matches) < 1:
        return
    for match in matches:
        entity_name = next((m for m in match if m), None)
        if entity_name:
            print(f"Matched {entity_name}")
            return entity_name

    print("Failed to match an entity")
    return
