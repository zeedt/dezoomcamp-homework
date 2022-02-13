{% macro get_month_from_number(value) %}
    
    case {{ value }}
        when 1 return 'JANUARY'
        when 2 return 'FEBRUARY'
        when 3 return 'MARCH'
        when 4 return 'APRIL'
        when 5 return 'MAY'
        when 6 return 'JUNE'
        when 7 return 'JULY'
        when 8 return 'AUGUST'
        when 9 return 'SEPTEMBER'
        when 10 return 'OCTOBER'
        when 11 return 'NOVEMBER'
        when 12 return 'DECEMBER'
    end

{%- endmacro %}