mysql_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type=='str' -%}
VARCHAR({{len}})
{%- elif type=='char' -%}
CHAR({{len}})
{%- elif type=='long' or type=='int' or type=='short' -%}
int
{%- elif type=='long' -%}
bigint
{%- elif type=='float' or type=='double' or type=='ddouble' -%}
double({{len}},{{dec}})
{%- elif type=='date' -%}
date
{%- elif type=='time' -%}
time
{%- elif type=='datetime' -%}
datetime
{%- elif type=='timestamp' -%}
TIMESTAMP
{%- elif type=='text' -%}
longtext
{%- elif type=='bin' -%}
longblob
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
{% if len(','.join(summary[0].primary))>0 %}
,primary key({{','.join(summary[0].primary)}})
{% endif %}
{% endmacro %}
drop table {{summary[0].name}};
CREATE TABLE {{summary[0].name}}
(
{% for field in fields %}
  {{field.name}} {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}} {%if field.title -%} comment '{{field.title}}'{%- endif %}{%- if not loop.last -%},{%- endif -%}
{% endfor %}
{{primary()}}
)
engine=innodb 
default charset=utf8 
{% if summary[0].title %}comment '{{summary[0].title}}'{% endif %}
;
{% for v in validation %}
{% if v.oper=='idx' %}
CREATE {% if v.value.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{",".join(v.value.fields)}});
{% endif %}
{%- endfor -%}
"""