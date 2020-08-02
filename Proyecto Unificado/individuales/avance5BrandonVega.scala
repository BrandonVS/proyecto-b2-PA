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

val canastaBasicaLoja2019 = data.filter("anio = 2019 and canton = 1101").withColumn("ingresos", expr("ingreso_laboral > 747.77"))
val canastaBasicaGuayaquil2019 = data.filter("anio = 2019 and canton = 0901").withColumn("ingresos", expr("ingreso_laboral > 730.24"))
val canastaBasicaCuenca2019 = data.filter("anio = 2019 and canton = 0101").withColumn("ingresos", expr("ingreso_laboral > 736.44"))

// COMMAND ----------

// Porcentaje de lojanos cuyo ingreso supera o es inferior a la canasta basica en el 2019
display(canastaBasicaLoja2019.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de guayaquileños cuyo ingreso supera o es inferior a la canasta basica en el 2019
display(canastaBasicaGuayaquil2019.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de cuencanos cuyo ingreso supera o es inferior a la canasta basica en el 2019
display(canastaBasicaCuenca2019.groupBy("ingresos").count)

// COMMAND ----------

val salarioBasico2015 = data.filter("anio = 2015").withColumn("ingresos",expr("ingreso_laboral > 354"))
val salarioBasico2016 = data.filter("anio = 2016").withColumn("ingresos",expr("ingreso_laboral > 366"))
val salarioBasico2017 = data.filter("anio = 2017").withColumn("ingresos",expr("ingreso_laboral > 375"))
val salarioBasico2018 = data.filter("anio = 2018").withColumn("ingresos",expr("ingreso_laboral > 386"))
val salarioBasico2019 = data.filter("anio = 2019").withColumn("ingresos",expr("ingreso_laboral > 394"))

// COMMAND ----------

// Porcentaje de ecuatorianos cuyo ingreso supera o es inferior al sueldo basico en el 2015
display(salarioBasico2015.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de ecuatorianos cuyo ingreso supera o es inferior al sueldo basico en el 2016
display(salarioBasico2016.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de ecuatorianos cuyo ingreso supera o es inferior al sueldo basico en el 2017
display(salarioBasico2017.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de ecuatorianos cuyo ingreso supera o es inferior al sueldo basico en el 2018
display(salarioBasico2018.groupBy("ingresos").count)

// COMMAND ----------

// Porcentaje de ecuatorianos cuyo ingreso supera o es inferior al sueldo basico en el 2019
display(salarioBasico2019.groupBy("ingresos").count)

// COMMAND ----------


