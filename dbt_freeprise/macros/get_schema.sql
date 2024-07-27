{% macro get_schema(node, default_schema) %}
    {% if node is not none and node.path is not none %}
        {% set model_path = node.path %}
        {% do log("Model path: " ~ model_path, info=True) %}

        {% if 'models/bronze' in model_path %}
            {% set schema = 'Bronze' %}
        {% elif 'models/silver' in model_path %}
            {% set schema = 'Silver' %}
        {% elif 'models/gold' in model_path %}
            {% set schema = 'Gold' %}
        {% else %}
            {% set schema = default_schema %}
        {% endif %}

        {% do log("Determined schema: " ~ schema, info=True) %}
        {{ return(schema) }}
    {% else %}
        {% if node is none %}
            {% do log("Node is undefined, using default schema: " ~ default_schema, info=True) %}
        {% elif node.path is none %}
            {% do log("Node path is undefined, using default schema: " ~ default_schema, info=True) %}
        {% endif %}
        {{ return(default_schema) }}
    {% endif %}
{% endmacro %}
