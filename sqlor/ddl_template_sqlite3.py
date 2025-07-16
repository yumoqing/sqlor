sqlite3_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type in ['str', 'char', 'date', 'time', 'datetime', 'timestamp'] -%}
TEXT
{%- elif type in ['long', 'int', 'short', 'longlong' ] -%}
int
{%- elif type in ['float', 'double', 'ddouble'] -%}
real
{%- elif type=='bin' -%}
blob
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
,primary key({{','.join(summary[0].primary)}})
{% endmacro %}
drop table if exists {{summary[0].name}};
CREATE TABLE {{summary[0].name}}
(
{% for field in fields %}
  `{{field.name}}` {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}}{%- if not loop.last -%},{%- endif -%} {%if field.title -%}  -- {{field.title}}{%- endif %}
{% endfor %}
{% if summary[0].primary and len(summary[0].primary)>0 %}
{{primary()}}
{% endif %}
)
{% if summary[0].title %} --{{summary[0].title}}{% endif %}
;
{% for v in indexes %}
CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{",".join(v.idxfields)}});
{%- endfor -%}
"""
