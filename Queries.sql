
with  producto_vendido as(
    select producto_id, max(valor_total) max_valor_total from database_prueba_tecnica.fact_venta
    group by producto_id limit 1 
)
select pro.producto_id, pro.desc_producto, producto_vendido.max_valor_total
from database_prueba_tecnica.dim_producto as pro
inner join producto_vendido on pro.producto_id = producto_vendido.producto_id
;


select pro.desc_producto, cli.numero, cli.ciudad, cli.departamento, cli.edad,
cli.nombre || ' ' || cli.primer_apellido || ' ' || cli.segundo_apellido as nombre_completo,
sum(fact.cantidad) cantidad, sum(fact.valor) valor, sum(fact.valor_total) valor_total
from database_prueba_tecnica.fact_venta fact
inner join database_prueba_tecnica.dim_cliente cli on fact.cliente_id = cli.cliente_id
inner join database_prueba_tecnica.dim_producto pro on fact.producto_id = pro.producto_id
group by pro.desc_producto, cli.numero, cli.ciudad, cli.departamento, cli.edad,
cli.nombre, cli.primer_apellido, cli.segundo_apellido
