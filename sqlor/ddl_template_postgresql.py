postgresql_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type=='str' -%}
VARCHAR({{len}})
{%- elif type=='char' -%}
CHAR({{len}})
{%- elif type=='long' or type=='int' or type=='short' -%}
NUMERIC(30,0)
{%- elif type=='float' or type=='double' or type=='ddouble' -%}
NUMERIC({{len}},{{dec}})
{%- elif type=='date' -%}
DATE
{%- elif type=='time' -%}
TIME
{%- elif type=='timestamp' -%}
TIMESTAMP
{%- else -%}
{{type}}
{%- endif %}
{%- endmacro %}
{% macro nullStr(nullable) %}
{%- if nullable=='no' -%}
NOT NULL
{%- endif -%}
{% endmacro %}
{% macro primary() %}
,PRIMARY KEY({{','.join(summary[0].primary)}})
{% endmacro %}
DROP TABLE IF EXISTS {{summary[0].name}};
CREATE TABLE {{summary[0].name}}
(
{% for field in fields %}
  {{field.name}} {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}}{%- if     not loop.last -%},{%- endif -%}
{% endfor %}
{% if summary[0].primary and len(summary[0].primary)>0 %}
{{primary()}}
{% endif %}
);
{% for v in indexes %}
CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{",".join(v.idxfields)}});
{%- endfor -%}
COMMENT ON TABLE {{summary[0].name}} IS '{{summary[0].title}}';
{% for field in fields %}
COMMENT ON COLUMN {{summary[0].name}}.{{field.name}} is '{{field.title}}';
{% endfor %}
"""
