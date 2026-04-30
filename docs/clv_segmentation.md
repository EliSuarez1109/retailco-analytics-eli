# Justificación de Segmentación de Clientes (CLV)

Para la creación del campo `value_segment` en el modelo `mart_customer_lifetime_value`, 
se ejecutó una consulta exploratoria sobre la tabla unificada de ventas utilizando la función analítica `PERCENTILE_CONT()`.

El objetivo fue dividir a la base de clientes en tres tercios exactos (terciles) basados en su gasto histórico total (Net Paid).

*   **Percentil 33 (Umbral Bajo-Medio):** $238,633.76
*   **Percentil 66 (Umbral Medio-Alto):** $309,725.43

**Reglas aplicadas:**
*   **High (Alto):** Clientes con un CLV superior a $309,725.43. Representan el 33% superior de nuestra facturación.
*   **Medium (Medio):** Clientes con un CLV entre $238,633.76 y $309,725.43.
*   **Low (Bajo):** Clientes con un CLV inferior a $238,633.76.