# Ejercicios de uso de un Dataset
Todos los siguiente ejercicios deberán realizarse con los datos del fichero CSV boston.csv presente en el 
directrio resources

## Cargar los datos desde el archivo CSV utilizando Java Spark:

Lee el archivo CSV utilizando la función spark.read().csv() y carga los datos en un DataFrame.
Asegúrate de especificar el esquema y los encabezados adecuados al leer el archivo CSV.
## Mostrar el esquema:

Utiliza la función dataFrame.printSchema() para mostrar los datos completos del esquema en la consola.
## Mostrar los datos:

Utiliza la función dataFrame.show() para mostrar los datos completos en la consola, mostrando todas las columnas y filas. 
## Manipulación de columnas:

- Utiliza la función dataFrame.columns() para obtener los nombres de las columnas presentes en el conjunto de datos.
- Utiliza la función dataFrame.columns().length para obtener la cantidad total de columnas.
## Extracción de datos:

- Utiliza la función dataFrame.take(n) para obtener los primeros 'n' registros del conjunto de datos.
- Utiliza la función dataFrame.select("nombreColumna") para mostrar una columna específica para todas las filas.
## Ordenación de datos

- Utiliza la función dataFrame.orderBy("nombreColumna") para ordenar el conjunto de datos en función de una columna específica, como el precio de la casa.
- Muestra los datos ordenados utilizando la función dataFrame.show().
## Creación de nuevas columnas

- Utiliza la función dataFrame.withColumn("nuevaColumna", expresion) para agregar una nueva columna que contenga una expresión basada en columnas existentes, como la suma de dos columnas.
- Muestra los datos con la nueva columna agregada utilizando la función dataFrame.show().
# Modificación de valores:

- Utiliza la función dataFrame.withColumn("nombreColumna", dataFrame.col("nombreColumna").plus(valor)) para aumentar el valor de una columna específica en un porcentaje determinado.
- Muestra los datos actualizados utilizando la función dataFrame.show().

## Eliminación de columnas:

- Utiliza la función dataFrame.drop("nombreColumna") para eliminar una columna específica del conjunto de datos.
- Muestra los datos sin la columna eliminada utilizando la función dataFrame.show().