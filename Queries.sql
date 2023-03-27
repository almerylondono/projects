
with  producto_vendido as(
    select producto_id, max(valor_total) max_valor_total from database_prueba_tecnica.fact_venta
    group by producto_id limit 1 
)
select pro.producto_id, pro.desc_producto, producto_vendido.max_valor_total
from database_prueba_tecnica.dim_producto as pro
inner join producto_vendido on pro.producto_id = producto_vendido.producto_id
;