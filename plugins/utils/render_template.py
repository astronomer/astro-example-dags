from jinja2 import BaseLoader, Environment

from plugins.utils.prefix_columns import prefix_columns
from plugins.utils.unprefix_columns import unprefix_columns


def render_template(template, context, extra_context={}):

    # Merge the new variables with the existing context to keep Airflow variables accessible
    combined_context = {**context, **extra_context}

    from pprint import pprint

    pprint(combined_context)
    env = Environment(loader=BaseLoader())
    env.filters["prefix_columns"] = prefix_columns
    env.filters["unprefix_columns"] = unprefix_columns

    # Manually render the template with the combined context
    # jinja_template = Template(template)
    jinja_template = env.from_string(template)

    output = jinja_template.render(**combined_context)

    return output
