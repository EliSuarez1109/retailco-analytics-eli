# Mapeo de Columnas: Unificación de Ventas y Devoluciones

Al unificar los canales de venta (Store, Catalog, Web), tanto en las ventas como en las devoluciones, nos encontramos con estructuras diferentes. Las siguientes decisiones de diseño se aplicaron para crear una tabla unificada coherente:

| Columna Unificada | Origen STORE | Origen CATALOG | Origen WEB | Justificación |
| :--- | :--- | :--- | :--- | :--- |
| **`sales_channel`** | *Hardcoded 'Store'* | *Hardcoded 'Catalog'* | *Hardcoded 'Web'* | Identificador para segmentar datos en los Marts si es de Store,Catalog o Web. |
| **`order_id`** | `ticket_number` | `order_number` | `order_number` | Son conceptualmente lo mismo: el identificador transaccional. |
| **`customer_id`** | `customer_id` | `bill_customer_id` | `bill_customer_id` | En envíos web/catálogo existen ship/bill. Unificamos usando el cliente que factura como el propietario real de la venta. |
| **`location_id`** | `store_id` | `call_center_id` | `web_site_id` | Generalizamos el concepto de lugar físico o virtual donde se originó la transacción. |
| **`warehouse_id`** | *NULL* | `warehouse_id` | `warehouse_id` | Las tiendas físicas venden inventario en sitio, no desde bodega, por lo que se asume que es NULL. |