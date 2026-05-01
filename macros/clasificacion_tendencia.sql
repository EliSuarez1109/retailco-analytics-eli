{#
    Propósito:
        Estandariza la lógica de negocio para clasificar tendencias en series temporales.
        Evita tener bloques CASE WHEN repetidos en múltiples marts y permite 
        cambiar la tolerancia en un solo lugar.

    Parámetros:
        - current_val (columna/numérico): El valor en el periodo actual.
        - previous_val (columna/numérico): El valor en el periodo anterior.
        - threshold_pct (float): Porcentaje de tolerancia para considerar la tendencia "Estable" (Por defecto 0.05 = 5%).

    Ejemplo de uso:
        SELECT {{ clasificacion_tendencia('avg_weekly_stock', 'prev_week_stock', 0.05) }} AS inventory_trend
#}

{% macro clasificacion_tendencia(current_val, previous_val, threshold_pct=0.05) %}
    CASE 
        WHEN {{ previous_val }} IS NULL THEN 'Sin historial'
        WHEN {{ current_val }} > {{ previous_val }} * (1 + {{ threshold_pct }}) THEN 'Creciente'
        WHEN {{ current_val }} < {{ previous_val }} * (1 - {{ threshold_pct }}) THEN 'Decreciente'
        ELSE 'Estable'
    END
{% endmacro %}