duckdb_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type in ['str', 'char'] -%}
VARCHAR({{len}})
{%- elif type in ['short', 'int'] -%}
INTEGER
{%- elif type == 'long' -%}
BIGINT
{%- elif type in ['float', 'double', 'ddouble'] -%}
DOUBLE
{%- elif type == 'date' -%}
DATE
{%- elif type == 'time' -%}
TIME
{%- elif type in ['datetime', 'timestamp'] -%}
TIMESTAMP
{%- elif type == 'text' -%}
VARCHAR
{%- elif type == 'bin' -%}
BLOB
{%- else -%}
{{type | upper}}
{%- endif %}
{%- endmacro %}

{%- macro defaultValue(defaultv) %}
{%- if defaultv %} DEFAULT '{{defaultv}}'{%- endif -%}
{%- endmacro %}

{% macro nullStr(nullable) %}
{%- if nullable=='no' -%}
NOT NULL
{%- endif -%}
{% endmacro %}

{% macro primary() %}
, PRIMARY KEY({{ ','.join(summary[0].primary) }})
{% endmacro %}

-- Drop table if exists
DROP TABLE IF EXISTS {{summary[0].name}};

-- Create table
CREATE TABLE {{summary[0].name}} (
{% for field in fields %}
  "{{field.name}}" {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}} {{defaultValue(field.default)}}{% if not loop.last %},{% endif %}
{% endfor %}
{% if summary[0].primary and len(summary[0].primary)>0 %}
{{primary()}}
{% endif %}
);

-- Table comment (DuckDB 不支持 COMMENT 语法)
{% if summary[0].title %}
-- COMMENT: {{summary[0].title}}
{% endif %}

-- DuckDB 当前版本不支持 CREATE INDEX 语法，以下保留作兼容注释
{% for v in indexes %}
-- INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{ ",".join(v.idxfields) }});
{% endfor %}
"""

