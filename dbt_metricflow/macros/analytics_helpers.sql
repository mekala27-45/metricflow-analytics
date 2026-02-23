{% macro generate_date_spine(start_date, end_date) %}
    with date_spine as (
        select
            unnest(generate_series(
                '{{ start_date }}'::date,
                '{{ end_date }}'::date,
                interval '1 day'
            )) as date_day
    )
    select date_day from date_spine
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} is null or {{ denominator }} = 0 then {{ default }}
        else {{ numerator }}::decimal / {{ denominator }}
    end
{% endmacro %}

{% macro cents_to_dollars(column_name) %}
    round({{ column_name }}::decimal / 100, 2)
{% endmacro %}

{% macro rolling_average(column, partition_by, order_by, window_size=7) %}
    avg({{ column }}) over (
        {% if partition_by %}partition by {{ partition_by }}{% endif %}
        order by {{ order_by }}
        rows between {{ window_size - 1 }} preceding and current row
    )
{% endmacro %}

{% macro growth_rate(current_val, previous_val) %}
    {{ safe_divide(current_val ~ ' - ' ~ previous_val, previous_val) }}
{% endmacro %}
