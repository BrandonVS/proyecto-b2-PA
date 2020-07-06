// Databricks notebook source
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

val dataFactor = data.select("factor_expansion")

// COMMAND ----------

data.select($"factor_expansion").groupBy("factor_expansion").count.sort($"factor_expansion").show()

// COMMAND ----------

dataFactor.count

// COMMAND ----------

dataFactor.summary().show()

// COMMAND ----------

dataFactor.select("factor_expansion").filter("factor_expansion = 4401.96321641217").count()

// COMMAND ----------

import org.apache.spark.sql.functions._
val avgFactor = dataFactor.select(avg($"factor_expansion")).first()(0).asInstanceOf[Double]

// COMMAND ----------

import org.apache.spark.sql.functions._
val stddevFactor = dataFactor.select(stddev($"factor_expansion")).first()(0).asInstanceOf[Double]

// COMMAND ----------

val lowerLimitFactor = avgFactor - 3 * stddevFactor 
val higherLimitFactor = avgFactor + 3 * stddevFactor

// COMMAND ----------

val lowerFactor = dataFactor.where($"factor_expansion" < lowerLimitFactor)
lowerFactor.describe().show()

// COMMAND ----------

val higherFactor = dataFactor.where($"factor_expansion" < higherLimitFactor)
higherFactor.describe().show()

// COMMAND ----------

data.select($"factor_expansion").where($"factor_expansion" > 969.870099662421).count()

// COMMAND ----------

val dataFactorOutLimits = dataFactor.where($"factor_expansion" > lowerLimitFactor && $"factor_expansion" < higherLimitFactor)
dataFactorOutLimits.summary().show
