// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
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

val provIngreso = data
                        .groupBy("provincia")
                        .pivot("anio")
                        .sum("ingreso_laboral")
                        .sort(asc("provincia"))
provIngreso.createOrReplaceTempView("tablaProvIngreso")

// COMMAND ----------

// MAGIC %sql
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

// MAGIC %sql
// MAGIC select * from tablaSueldo

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
provinciaIngreso.createOrReplaceTempView("tablaProvGrupo")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tablaProvGrupo
