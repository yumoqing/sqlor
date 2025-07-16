sqlserver_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type=='str' -%}
NVARCHAR({{len}})
{%- elif type=='char' -%}
CHAR({{len}})
{%- elif type=='long' or type=='int' or type=='short' -%}
NUMERIC
{%- elif type=='float' or type=='double' or type=='ddouble' -%}
numeric({{len}},{{dec}})
{%- elif type=='date' or type=='time' -%}
DATE
{%- elif type=='timestamp' -%}
TIMESTAMP
{%- elif type=='text' -%}
NVARCHAR(MAX)
{%- elif type=='bin' -%}
IMAGE
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

drop table dbo.{{summary[0].name}};
CREATE TABLE dbo.{{summary[0].name}}
(
{% for field in fields %}
  {{field.name}} {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}}{%- if not loop.last -%},{%- endif -%}
{% endfor %}
{% if summary[0].primary and len(summary[0].primary)>0 %}
{{primary()}}
{% endif %}
)
{% for v in indexes %}
CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{",".join(v.idxfields)}});
{%- endfor -%}
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{{summary[0].title}}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{{summary[0].name}}'
{% for field in fields %}
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{{field.title}}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{{summary[0].name}}', @level2type=N'COLUMN',@level2name=N'{{field.name}}'
{% endfor %}
"""
