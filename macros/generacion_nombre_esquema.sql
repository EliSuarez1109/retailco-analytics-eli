{#
    Propósito:
        Sobreescribe la macro nativa de dbt para evitar que concatene el nombre 
        del esquema por defecto del usuario (DBT_ELISUAREZ) con el esquema que vamos a utilizar.
        Garantiza que las tablas se creen exactamente en el esquema indicado.

    Parámetros:
        - custom_schema_name (string): El nombre del esquema definido en el dbt_project.yml
        - node (dict): El nodo actual que dbt está procesando.

    Ejemplo de uso:
        En dbt_project.yml:
        models:
            marts:
                +schema: marts
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}