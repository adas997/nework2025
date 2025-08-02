{% test primary_key (model,column_name) %}

select *
from
(
select {{column_name}},
       row_number() over(partition by {{column_name}} order by {{column_name}} ) rn
from {{model}}
)
where rn > 1 or {{column_name}} is null


{% endtest %}