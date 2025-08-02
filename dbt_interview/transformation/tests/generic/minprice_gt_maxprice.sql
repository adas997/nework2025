{% test minprice_gt_maxprice (model,column_name,field) %}
 
 {{ config(severity = 'warn') }}
 
select *
from {{model}}
where {{column_name}} > {{field}}

{% endtest %}
