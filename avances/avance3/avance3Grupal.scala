// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.types._
val myDataSchema = StructType(
    Array(
        StructField("id",DecimalType(26,0), true),
        StructField("anio", IntegerType, true),
        StructField("mes", IntegerType, true),
        StructField("provincia", IntegerType,true),
        StructField("canton", IntegerType, true),
        StructField("area", StringType, true),
        StructField("genero", StringType, true),
        StructField("edad", IntegerType, true),
        StructField("estado_civil", StringType, true),
        StructField("nivel_de_instruccion", StringType, true),
        StructField("etnia", StringType, true),
        StructField("ingreso_laboral", IntegerType, true),
        StructField("condicion_actividad", StringType, true),
        StructField("sectorizacion", StringType, true),
        StructField("grupo_ocupacion", StringType, true),
        StructField("rama_actividad", StringType, true),
        StructField("factor_expansion",DoubleType,true)
        )
    );

// COMMAND ----------

val data = spark
.read
.schema(myDataSchema)
.option("header", "true")
.option("delimiter","\t")
.csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

val totalDatos = data.count
val data2019 = data.filter("anio = 2019").count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Consecuencias a nivel de ingreso economico ocasionado por el terremoto de 2016 en la provincia de Esmeraldas y zonas cercanas 

// COMMAND ----------

//Esmeraldas
data
.filter("provincia = 8")
.groupBy("anio")
.agg(sum("ingreso_laboral").alias("IngresoEsmeraldas"))
.withColumn("aux", col("IngresoEsmeraldas")*100 /  sum("IngresoEsmeraldas").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

//Pichincha
data
.filter("provincia = 17")
.groupBy("anio")
.agg(sum("ingreso_laboral").alias("IngresoPichincha"))
.withColumn("aux", col("IngresoPichincha")*100 /  sum("IngresoPichincha").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

//Manabi
data
.filter("provincia = 13")
.groupBy("anio")
.agg(sum("ingreso_laboral").alias("IngresoManabi"))
.withColumn("aux", col("IngresoManabi")*100 /  sum("IngresoManabi").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

//Imbabura
data
.filter("provincia = 10")
.groupBy("anio")
.agg(sum("ingreso_laboral").alias("IngresoImbabura"))
.withColumn("aux", col("IngresoImbabura")*100 /  sum("IngresoImbabura").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

//Carchi
data
.filter("provincia = 4")
.groupBy("anio")
.agg(sum("ingreso_laboral").alias("IngresoCarchi"))
.withColumn("aux", col("IngresoCarchi")*100 /  sum("IngresoCarchi").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

// COMMAND ----------

// MAGIC %md
// MAGIC Conclusión: El filtrado de información muestra que tanto las provincias de Pichincha, Manabi si bien tuvieron un deceso a nivel de ingreso laboral en el año 2016, los posteriores años volvieron estavilizarce.
// MAGIC Por otro lado las provincias de Carchi y Esmeraldas, muestran bajas considerables en los dos ultimos años. Finalmente  La provincia de Imbabura no sufrio un descenso ese año y los descensos mostrados en 2018 y 2019 pueden ser externos 

// COMMAND ----------

import org.apache.spark.sql.functions._
data
.filter("grupo_ocupacion is null")
.groupBy("anio").count()
.withColumn("aux", col("count")*100 / sum("count").over())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"anio").show

// COMMAND ----------

// MAGIC %md 
// MAGIC # Cuantas Personas pueden pagar pagar la canasta basica ($716.14) en el ultimo anio registrado
// MAGIC [Precio actual de la canasta basica (pag 10)](https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/2020/Enero-2020/Boletin_tecnico_01-2020-IPC.pdf)

// COMMAND ----------

data
.filter("ingreso_laboral >= 716.14 and anio = 2019")
.agg(count("ingreso_laboral").alias("Igual o mayor a la canasta"))
.withColumn("aux", col("Igual o mayor a la canasta")*100 /  data2019)
.withColumn("Promedio",round($"aux",2))
.drop("aux")
.show()

// COMMAND ----------

data
.filter("ingreso_laboral >= 400 and anio = 2019")
.agg(count("ingreso_laboral").alias("Superior Sueldo min"))
.withColumn("aux", col("Superior Sueldo min")*100 /  data2019)
.withColumn("Promedio",round($"aux",2))
.drop("aux")
.show()

// COMMAND ----------

// Sueldo en funcion de la provincia
// Provincia de Loja
println("Provincia de Loja")
data.filter("provincia like 11").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)
// Canton Loja
println("Canton Loja")
data.filter("provincia like 11 and canton like 1101").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)
// Provincia Azuay
println("Provincia Azuay")
data.filter("provincia like 01").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)
// Canton Cuenca
println("Canton Cuenca")
data.filter("provincia like 01 and canton like 0101").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)
// Provincia Guayas
println("Provincia Guayas")
data.filter("provincia like 09").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)
// Canton Guayaquil
println("Canton Guayaquil")
data.filter("provincia like 09 and canton like 0901").groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)


// COMMAND ----------

// Porcentaje de ingreso por trabajo a nivel nacional
data
.groupBy("grupo_ocupacion")
.agg(sum("ingreso_laboral").alias("Ingreso nacional"))
.withColumn("Porcentaje", col("Ingreso nacional")*100 /  sum("Ingreso nacional").over()).sort(desc("Porcentaje")).show


// COMMAND ----------

// Porcentaje de ingreso por trabajo por provincia
println("Provincia Loja")
data.filter("provincia like 11").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso provincia Loja")).withColumn("Porcentaje", col("Ingreso provincia Loja")*100 /  sum("Ingreso provincia Loja").over()).sort(desc("Porcentaje")).show
println("Canton Loja")
data.filter("provincia like 11 and canton like 1101").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso canton Loja")).withColumn("Porcentaje", col("Ingreso canton Loja")*100 /  sum("Ingreso canton Loja").over()).sort(desc("Porcentaje")).show
println("Provincia Azuay")
data.filter("provincia like 01").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso provincia Azuay")).withColumn("Porcentaje", col("Ingreso provincia Azuay")*100 /  sum("Ingreso provincia Azuay").over()).sort(desc("Porcentaje")).show
println("Canton Cuenca")
data.filter("provincia like 01 and canton like 0101").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso canton Cuenca")).withColumn("Porcentaje", col("Ingreso canton Cuenca")*100 /  sum("Ingreso canton Cuenca").over()).sort(desc("Porcentaje")).show
println("Provincia Guayas")
data.filter("provincia like 09").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso provincia Guayas")).withColumn("Porcentaje", col("Ingreso provincia Guayas")*100 /  sum("Ingreso provincia Guayas").over()).sort(desc("Porcentaje")).show
println("Canton Guayaquil")
data.filter("provincia like 09 and canton like 0901").groupBy("grupo_ocupacion").agg(sum("ingreso_laboral").alias("Ingreso canton Guayaquil")).withColumn("Porcentaje", col("Ingreso canton Guayaquil")*100 /  sum("Ingreso canton Guayaquil").over()).sort(desc("Porcentaje")).show


// COMMAND ----------


