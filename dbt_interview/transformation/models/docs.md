{% docs dim_mart_accounts__ %}
Account Dimension  - Containing Account, Contact dimensional attributes and corresponding IDs. It has account_sk as hashed (PK)
{% enddocs %}


{% docs dim_mart_cases__ %}
Case Dimension  - Gets data from case staging views.
{% enddocs %}

{% docs dim_mart_opportunities__ %}
Dimension that lists the descriptive attributes for opportunity. opportunity_sk is hashed and is treated as the Surrogate Key !
{% enddocs %}


{% docs dim_mart_products__ %}
Dimension that lists the descriptive attributes for Products. product_sk is hashed and is treated as the Surrogate Key !
{% enddocs %}

{% docs dim_mart_users__ %}
Dimension that lists the descriptive attributes for Users. user_sk is hashed and is treated as the Surrogate Key !
{% enddocs %}


{%  fct_mart_prod_price__ %}
This is the fact table that contains aggregated product price measures and related ids. Joined to product and price dimension 
{% enddocs %}

{%  fct_mart_revenues__ %}
Fact Table that contains aggregated  revenue earned, total opportunity amount , revenue expected etc. Joined to account and opportunity dimension
{% enddocs %}