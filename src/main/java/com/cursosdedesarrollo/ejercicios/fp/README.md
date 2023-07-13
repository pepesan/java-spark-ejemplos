# Ejercicios de uso de programación funcional en Spark

En todos los ejercicios se usará una estructura similar a la siguiente
## Datos iniciales
<code>
Dataset<Row> ventas = sparkSession.createDataFrame(Arrays.asList(
RowFactory.create("Producto A", 10),
RowFactory.create("Producto B", 5),
RowFactory.create("Producto C", 8),
RowFactory.create("Producto A", 3),
RowFactory.create("Producto C", 6)
), new StructType()
.add("producto", DataTypes.StringType)
.add("unidades_vendidas", DataTypes.IntegerType));
</code>

## Ejercicio 1:
Dado un conjunto de datos que contiene información sobre ventas, encuentra la cantidad total de unidades vendidas utilizando el método reduce.
## Ejercicio 2:
Dado un conjunto de datos que contiene información sobre ventas, encuentra el producto con el precio mínimo utilizando el método min.
## Ejercicio 3:
Dado un conjunto de datos que contiene información sobre ventas, encuentra el producto con el precio máximo utilizando el método max.
## Ejercicio 4:
Dado un conjunto de datos que contiene información sobre ventas, calcula el precio promedio de todos los productos utilizando el método avg.
## Ejercicio 5:
Dado un conjunto de datos que contiene información sobre ventas, calcula la suma total de precios utilizando el método reduce y una función lambda.
