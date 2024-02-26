from pprint import pprint

from jinja2 import Template


def render_template(template, context, extra_context={}):

    # Merge the new variables with the existing context to keep Airflow variables accessible
    combined_context = {**context, **extra_context}

    print("extra_context", extra_context)
    pprint(combined_context)
    # Manually render the template with the combined context
    jinja_template = Template(template)
    output = jinja_template.render(**combined_context)

    return output
