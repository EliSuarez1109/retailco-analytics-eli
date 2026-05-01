# RetailCo Analytics - dbt Project

## Descripción del Proyecto
Este proyecto construye un Data Warehouse moderno utilizando **dbt** y **Snowflake** para la empresa RetailCo. El objetivo principal es transformar datos crudos del dataset TPC-DS en modelos analíticos limpios, estandarizados y testeados, listos para ser consumidos por herramientas de Business Intelligence como Power BI.

El proyecto responde a preguntas clave de negocio sobre ventas por varios canales, valor real del cliente y salud del inventario.

---

## Arquitectura y Decisiones de Diseño

El proyecto sigue las mejores prácticas de la **Arquitectura Medallion** dividida en tres capas físicas y lógicas:

1. **Staging (Capa Bronce/Plata):**
   - Vistas materializadas (`view`) que extraen datos de la base de datos `RAW`.
   - Estandarización de nombres de columnas (snake_case), casteo de tipos de datos y manejo de nulos.
2. **Intermediate (Capa Plata):**
   - Tablas y vistas que resuelven la lógica de negocio compleja (ej. unificar ventas físicas, web y de catálogo en un solo modelo).
3. **Marts (Capa Oro):**
   - Tablas materializadas (`table` e `incremental`) altamente desnormalizadas y optimizadas para la lectura en herramientas de BI.

### Decisiones Técnicas Destacadas
- **Materializaciones Incrementales:** Se configuró el modelo `mart_sales_by_channel` como incremental para procesar únicamente los datos de los últimos meses, reduciendo los costos de cómputo en Snowflake.
- **Macros Personalizadas:** Se utilizó Jinja para crear macros de negocio y macros de arquitectura para sobreescribir el comportamiento por defecto de dbt y forzar la creación de esquemas limpios.
- **Segmentación Dinámica:** Los umbrales de la métrica CLV (High, Medium, Low) no fueron introducidos manualmente, sino que se calcularon previamente mediante consultas de distribución analítica (`PERCENTILE_CONT`) sobre los datos reales.

---

## Modelos Analíticos Principales (Marts)

*   **`mart_sales_by_channel`:** Métricas agregadas de ventas (ingresos brutos, netos, descuentos) y devoluciones desglosadas por canal (Store, Web, Catalog), ubicación, categoría y mes.
*   **`mart_customer_lifetime_value`:** Visión 360 del cliente. Incluye el número de transacciones por canal, su primera y última compra, su tasa de devolución histórica y su segmento de valor (CLV).
*   **`mart_inventory_health`:** Análisis semanal de stock cruzado con ventas reales. Incluye métricas de semanas de stock, días de rotura de inventario y evaluación de tendencia usando Window Functions.

---

## Calidad de Datos (Testing)

El proyecto cuenta con una suite de validación robusta para asegurar la integridad de los datos:
- **Tests Genéricos:** `not_null`, `unique`, `accepted_values`.
- **Tests de dbt_expectations:** Validaciones avanzadas como verificación de rangos numéricos (`expect_column_values_to_be_between`), comparaciones de columnas (`expect_column_pair_values_A_to_be_greater_than_or_equal_to_B`) y longitud de strings.
- **Tests Singulares (Personalizados):** Consultas SQL propias para asegurar reglas de negocio estrictas, como verificar que ningún cliente con CLV positivo tenga cero transacciones registradas, o que la suma del staging cuadre exactamente con la del mart final.

---

## Instrucciones para reproducir el proyecto

### Prerrequisitos
- Tener instalado dbt-core y el adaptador dbt-snowflake.
- Acceso a una cuenta de Snowflake con los datos crudos de TPC-DS.
- Configurar el archivo `~/.dbt/profiles.yml` apuntando a tu base de datos y esquema de desarrollo.

### Paso a paso

1. **Clonar el repositorio y navegar a la carpeta:**
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd retailco-analytics-eli

2. **Instalar las dependencias:**
   ```bash
   dbt deps 

3. **Ejecutar las transformaciones por capas:**
   ```bash
   dbt run --select staging
   dbt run --select intermediate
   dbt run --select marts

4. **Ejecutar test de validación:**
   ```bash
   dbt test

5. **Generar la documentación interactiva:**
   ```bash
   dbt compile --write-catalog
   curl -o target/index.html https://raw.githubusercontent.com/dbt-labs/dbt-core/main/core/dbt/task/docs/index.html
  

6. **Visualizar la documentación interactiva:**
   ```bash
   python3 -m http.server 8080 --directory target
   
   ```
   Luego abre tu navegador y entra a: http://localhost:8080