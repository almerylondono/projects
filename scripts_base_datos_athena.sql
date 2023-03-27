DROP TABLE IF EXISTS database_prueba_tecnica.dim_cliente;

CREATE EXTERNAL TABLE database_prueba_tecnica.dim_cliente(
  cliente_id 		bigint, 
  cod_seguridad 	string, 
  numero 			string, 
  nombre 			string, 
  primer_apellido 	string, 
  segundo_apellido 	string, 
  ciudad 			string, 
  departamento 		string, 
  edad 				bigint, 
  fecha_ejecucion 	timestamp)
STORED AS PARQUET 
LOCATION
  's3://dllo-analytics-prueba-tecnica/financiera/dim_cliente'
;

DROP TABLE IF EXISTS database_prueba_tecnica.dim_producto;

CREATE EXTERNAL TABLE database_prueba_tecnica.dim_producto(
  producto_id		bigint, 
  desc_producto 	string, 
  fecha_ejecucion 	timestamp)
STORED AS PARQUET 
LOCATION
  's3://dllo-analytics-prueba-tecnica/financiera/dim_producto'
;

DROP TABLE IF EXISTS database_prueba_tecnica.fact_venta;

CREATE EXTERNAL TABLE database_prueba_tecnica.fact_venta(
  producto_id 	bigint, 
  cliente_id 	bigint, 
  cantidad 		decimal(10,2), 
  valor 		decimal(16,6),
  valor_total	decimal(16,6)
  )
STORED AS PARQUET
LOCATION
  's3://dllo-analytics-prueba-tecnica/financiera/fact_venta'
 ;
  
