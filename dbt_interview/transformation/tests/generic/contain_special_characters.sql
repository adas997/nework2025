{% test contain_special_characters (model,column_name) %}

select *
from {{model}}
where REGEXP_MATCHES({{column_name}}, '^[a-zA-Z]+$')

{% endtest %}