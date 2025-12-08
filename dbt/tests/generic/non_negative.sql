{% test non_negative(model, column_name) %}

-- Test genérico: los valores deben ser NO NEGATIVOS y NO NULOS.
-- El test pasa si esta consulta devuelve CERO filas.

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} is null
   or {{ column_name }} < 0

{% endtest %}
