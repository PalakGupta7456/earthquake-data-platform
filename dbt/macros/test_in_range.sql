{% test in_range(model, column_name, min_value, max_value) %}
-- Fails for any row outside [min_value, max_value].
select * from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
