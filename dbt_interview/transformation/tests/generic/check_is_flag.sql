{% test check_is_flag (model,column_name) %}

 {{ config(severity = 'warn') }}

select *
from {{model}}
where  {{column_name}} < 0 or {{column_name}} > 1 
    or instr(cast({{column_name}} as varchar) , '.') > 0 or  REGEXP_MATCHES(cast({{column_name}} as varchar), '[a-zA-Z]') 

{% endtest %}