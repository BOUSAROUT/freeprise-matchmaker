{% macro get_schema(default_schema) %}
    {% set model_path = node.path %}
    {% if 'models/bronze' in model_path %}
        {% set schema = 'Bronze' %}
    {% elif 'models/silver' in model_path %}
        {% set schema = 'Silver' %}
    {% elif 'models/gold' in model_path %}
        {% set schema = 'Gold' %}
    {% else %}
        {% set schema = default_schema %}
    {% endif %}
    {{ return(schema) }}
{% endmacro %}
