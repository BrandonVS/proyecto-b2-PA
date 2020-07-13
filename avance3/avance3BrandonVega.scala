// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val myDataSchemaDDL = "id INT,anio INT, mes INT, provincia INT, canton INT, area STRING, genero STRING, edad INT, estado_civil STRING, nivel_de_instruccion STRING, etnia STRING, ingreso_laboral INT, condicion_actividad STRING, sectorizacion STRING, grupo_ocupacion STRING, rama_actividad STRING, factor_expansion DOUBLE"

// COMMAND ----------

val data = spark
    .read
    .schema(myDataSchemaDDL)
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

// Sueldo promedio de empleos listados
data.groupBy("grupo_ocupacion").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")).show(false)

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
