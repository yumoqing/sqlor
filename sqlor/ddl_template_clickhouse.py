clickhouse_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type in ['str', 'char', 'text'] -%}
String
{%- elif type in ['short', 'int'] -%}
Int32
{%- elif type == 'long' -%}
Int64
{%- elif type in ['float', 'double', 'ddouble'] -%}
Float64
{%- elif type == 'date' -%}
Date
{%- elif type in ['datetime', 'timestamp'] -%}
DateTime
{%- elif type == 'bin' -%}
String
{%- else -%}
{{ type | upper }}
{%- endif %}
{%- endmacro %}

{% macro defaultValue(defaultv) %}
{%- if defaultv %} DEFAULT '{{defaultv}}'{%- endif -%}
{%- endmacro %}

{% macro nullStr(nullable) %}
{%- if nullable == 'no' -%}
NOT NULL
{%- else -%}
NULL
{%- endif -%}
{% endmacro %}

{% macro primary() %}
PRIMARY KEY ({{ ','.join(summary[0].primary) }})
ORDER BY ({{ ','.join(summary[0].primary) }})
{% endmacro %}

DROP TABLE IF EXISTS {{ summary[0].name }};

CREATE TABLE {{ summary[0].name }} (
{% for field in fields %}
  `{{ field.name }}` {{ typeStr(field.type, field.length, field.dec) }} {{ nullStr(field.nullable) }} {{ defaultValue(field.default) }}{% if field.title %} COMMENT '{{field.title}}'{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
)
ENGINE = MergeTree()
{% if summary[0].primary and len(summary[0].primary) > 0 %}
{{ primary() }}
{% else %}
ORDER BY tuple()
{% endif %}
{% if summary[0].title %}
COMMENT '{{ summary[0].title }}'
{% endif %}
;
"""

