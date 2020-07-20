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

val numData = data.count().toDouble

// COMMAND ----------

// MAGIC %md
// MAGIC #Revision de Data 

// COMMAND ----------

data.select("ingreso_laboral", "edad").summary().show()

// COMMAND ----------

data.select("ingreso_laboral").groupBy("ingreso_laboral").count.sort($"ingreso_laboral".desc).show(5)
data.select("edad").groupBy("edad").count.sort($"edad".desc).show(5)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Como se puede visualizar existen valores muy altos en las columnas ingreso_labiral y anio que alteran el promedio, por lo que es necesario realizar una eliminación de *outliers*
// MAGIC #### Método de la Desviación Estandar 

// COMMAND ----------

val dfIngresoLaboral = data.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val dataAge = data.select("edad")

// COMMAND ----------

// ingreso_laboral

val avg = dfIngresoLaboral.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stvDesv = dfIngresoLaboral.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]

// Limites

val inferior = avg -3* stvDesv
val superior = avg +3* stvDesv 

// edad 

val avgAge = dataAge.select(mean("edad")).first()(0).asInstanceOf[Double]
val stddevAge = dataAge.select(stddev("edad")).first()(0).asInstanceOf[Double]

// Limites

val lowerLimit = avgAge -3* stddevAge 
val higherLimit = avgAge +3* stddevAge 

// COMMAND ----------

val ingresoLaboralSinOutliers = dfIngresoLaboral.where($"ingreso_laboral">inferior && $"ingreso_laboral" < superior)
ingresoLaboralSinOutliers.select($"ingreso_laboral").summary().show()

// COMMAND ----------

val dataAgesOutLimits = dataAge.where($"edad"> lowerLimit && $"edad"< higherLimit)
dataAgesOutLimits.select($"edad").summary().show()

// COMMAND ----------

// DBTITLE 1,Agrupación de la información en función de las categorias existentes y la cantidad de personas pertenecientes a esta. 
//Agrupación de la información en función de las categorias existentes y la cantidad de personas pertenecientes a esta. 
import org.apache.spark.sql.functions._
data
.filter("grupo_ocupacion is not null")
.groupBy("grupo_ocupacion")
.agg(count("grupo_ocupacion").alias("Numero"))
.withColumn("aux", col("Numero")*100 /  numData)
.withColumn("Promedio",round($"aux",2))
.drop("aux")
.sort($"Numero".desc)
.show(false)

// COMMAND ----------

// DBTITLE 1,Edad más común entre las personas que han llenado la columna grupo_ocupacion
dataAgesOutLimits
.filter("grupo_ocupacion is not null")
.groupBy("edad")
.agg(count("edad").alias("Personas"))
.withColumn("aux", col("Personas")*100 /  dataAgesOutLimits.count())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"Porcentaje".desc)
.show(false)

// COMMAND ----------

// DBTITLE 1,Número de personas que conforman cada grupo ocupación, divididas en años 
val grupoOcupacionporAnio = data.groupBy("anio").pivot("grupo_ocupacion").count().orderBy("anio")
grupoOcupacionporAnio.createOrReplaceTempView("grupoOcupacionporAnioView")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from grupoOcupacionporAnioView order by anio

// COMMAND ----------

// DBTITLE 1,Número de personas que dejaron vacias la columna grupo_ocupacion y agrupada en Años
print("Nulos \n")
data.select($"anio").filter("grupo_ocupacion is null").groupBy("anio").count().sort($"anio".desc).show()

// COMMAND ----------

// DBTITLE 1,No pertenecen a un grupo y tienen sueldo
data.filter("grupo_ocupacion is null and ingreso_laboral > 0").groupBy("anio").count.show

// COMMAND ----------

// MAGIC %md
// MAGIC #### Durante los años 2015 y 2016 existen varios cientos de registros que aunque su grupo de ocupación es *nulo* tienen un ingreso laboral, por lo que se deduce que estas irregularidades son producto de ocupaciones no registradas/contempladas en los grupos originales y que estas fueron incluidas en años posteriores 

// COMMAND ----------

val grupoOcupacionporProvincia = data.groupBy("provincia").pivot("grupo_ocupacion").count().orderBy("provincia")
grupoOcupacionporProvincia.createOrReplaceTempView("grupoOcupacionporProvincia")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from grupoOcupacionporProvincia

// COMMAND ----------

// MAGIC %md
// MAGIC #### Distribución del Grupo de Ocupación en los Cantones más costosos del País 
// MAGIC [Enlace de Referencia](https://www.eluniverso.com/noticias/2020/01/07/nota/7679187/loja-ciudad-mas-cara-ecuador-inec-2019)

// COMMAND ----------

val gOporCantonesCaros = data.filter("canton like 0101 or canton like 1101 or canton like 0901").groupBy("grupo_ocupacion").pivot("canton").count().orderBy("grupo_ocupacion")
gOporCantonesCaros.createOrReplaceTempView("gOporCantonesCaros")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from gOporCantonesCaros order by 1 desc 

// COMMAND ----------

// DBTITLE 1,Frecuncia de las personas según el grupo de ocupación y a un anio determinado 
val gOporEdadesCruciales = data.filter("edad = 25 or edad =30 or edad =35").groupBy("grupo_ocupacion").pivot("edad").count().orderBy("grupo_ocupacion")
gOporEdadesCruciales.createOrReplaceTempView("gOporEdadesCruciales")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from gOporEdadesCruciales

// COMMAND ----------

// DBTITLE 1,Distribución de las personas de tercera edad personas segun la rama la rama de actividades 
val gOporTerceraEdad = data.filter("edad >=65").groupBy("rama_actividad").count().orderBy($"count".desc)
gOporTerceraEdad.createOrReplaceTempView("gOporTerceraEdad")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from gOporTerceraEdad 

// COMMAND ----------

val provIngreso = data
                        .groupBy("provincia")
                        .pivot("anio")
                        .sum("ingreso_laboral")
                        .sort(asc("provincia"))
provIngreso.createOrReplaceTempView("tablaProvIngreso")

// COMMAND ----------

// MAGIC  %sql
// MAGIC select * from tablaProvIngreso

// COMMAND ----------

val grupoNull = data
                .filter("grupo_ocupacion is null")
                .groupBy("anio").count()
                .withColumn("aux", col("count")*100 / sum("count").over())
                .withColumn("Porcentaje",round($"aux",2))
                .drop("aux")
                .sort($"anio")
grupoNull.createOrReplaceTempView("tablaGrupoNull")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tablaGrupoNull

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

val sueldoProvincia = data
                      .groupBy("provincia")
                      .avg("ingreso_laboral")
                      .sort(asc("provincia"))
sueldoProvincia.createOrReplaceTempView("tablaSueldo")

// COMMAND ----------

// MAGIC  %sql
// MAGIC  select * from tablaSueldo

// COMMAND ----------

val grupoIngreso = data
                    .groupBy("grupo_ocupacion")
                    .agg(sum("ingreso_laboral").alias("Ingreso nacional"))
                    .withColumn("Porcentaje", col("Ingreso nacional")*100 /  sum("Ingreso nacional").over()).sort(desc("Porcentaje"))
grupoIngreso.createOrReplaceTempView("tablaGrupoIngreso")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tablaGrupoIngreso

// COMMAND ----------

val provinciaGrupo = data
                        .groupBy("provincia")
                        .pivot("grupo_ocupacion")
                        .sum("ingreso_laboral")
                        .sort(asc("provincia"))
provinciaGrupo.createOrReplaceTempView("tablaProvGrupo")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tablaProvGrupo

// COMMAND ----------


