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

// DBTITLE 1,Desviación Estándar
// MAGIC %md
// MAGIC ###Obtener la desviación estandar de todas las columnas a las que se pueden realizar (*ingreso_laboral,edad,factor_expansion*)

// COMMAND ----------

// DBTITLE 1,Número de filas en total 
println(s"Total de filas : ${data.count}")

// COMMAND ----------

// DBTITLE 1,Uso del método summary en las columna ingreso_laboral
data.select("ingreso_laboral").summary().show()

// COMMAND ----------

// DBTITLE 1,Agrupación de los valores 
data.select("ingreso_laboral").groupBy("ingreso_laboral").count.sort($"ingreso_laboral".desc).show(5)

// COMMAND ----------

// DBTITLE 1,Creación de un nuevo dataFrame para trabajar los datos de ingreso_laboral no nulos
val dfIngresoLaboral = data.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)

// COMMAND ----------

// DBTITLE 1,Divisón de los resultados por rangos 
val cantValoresEnDRangos= scala.collection.mutable.ListBuffer[Long]()
val minValue = 0
val maxValue= 146030
val bins =5 
val range = (maxValue - minValue)/bins
var minCounter = minValue
var maxCounter = range
while(minCounter < maxValue){
  val rangeValues = dfIngresoLaboral.where($"ingreso_laboral".between(minCounter, maxCounter))
  cantValoresEnDRangos.+=(rangeValues.count())
  minCounter = maxCounter
  maxCounter = maxCounter+ range
}
print("Valores en diferentes Rangos")
cantValoresEnDRangos.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ###En el Caso de *databricks* se debe importar la librerías *functions* para poder usar *avg,stddev* en el select 

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Cálculo del promedio
val avg = dfIngresoLaboral.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

// DBTITLE 1,Cálculo Desviación Estandar
val stvDesv = dfIngresoLaboral.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

val inferior = avg -3* stvDesv
val superior = avg +3* stvDesv 

// COMMAND ----------

// DBTITLE 1,Valores menores 
val valoresMenores = dfIngresoLaboral.where($"ingreso_laboral"< inferior)
valoresMenores.summary().show()

// COMMAND ----------

// DBTITLE 1,Valores superiores
val valoresMayores =  dfIngresoLaboral.where($"ingreso_laboral"> superior)
valoresMayores.summary().show()

// COMMAND ----------

// DBTITLE 1,Data eliminando los Outliers 
val datasinOutliers = dfIngresoLaboral.where($"ingreso_laboral">inferior && $"ingreso_laboral" < superior)
datasinOutliers.select($"ingreso_laboral").summary().show()

// COMMAND ----------

// DBTITLE 1,Outliers en las columnas ingreso_laboral y edad 
// MAGIC %md
// MAGIC ###Las columnas en las cuales se puede aplicar el proceso outliers son dos ingreso laboral y anio, paral os cuales verificaremos la cantidad de datos peculiares se encuentras y si alteran o no la estadística de la dataset 

// COMMAND ----------

// MAGIC %md
// MAGIC #Columna edad

// COMMAND ----------

// DBTITLE 1,Observación de la data
dataAge.select($"edad").groupBy("edad").count.sort($"edad").show()

// COMMAND ----------

// DBTITLE 1,Creación de un nuevo dataFrame 
val dataAge = data.select("edad")

// COMMAND ----------

dataAge.count

// COMMAND ----------

dataAge.summary().show()

// COMMAND ----------

// DBTITLE 1,división de la data en rangos 
val valuesRange= scala.collection.mutable.ListBuffer[Long]()
val minValue = 15
val maxValue= 99
val bins =6
val range = (maxValue - minValue)/bins
var minCounter = minValue
var maxCounter = minValue+range
while(minCounter < maxValue){
  val rangeValues = dataAge.where($"edad".between(minCounter, maxCounter))
  valuesRange.+=(rangeValues.count())
  minCounter = maxCounter
  maxCounter = maxCounter+ range
}
print("Valores en diferentes Rangos \n")
valuesRange.foreach(println)

// COMMAND ----------

// DBTITLE 1,Resultados 
// MAGIC %md
// MAGIC | Rango Edad | Cantidad   |
// MAGIC | ---------- | ---------- |
// MAGIC | 15-28      | 176299     |
// MAGIC | 29-42      | 209627     |
// MAGIC | 43-56      | 165095     |
// MAGIC | 57-70      | 84262      |
// MAGIC | 71-84      | 23058      |
// MAGIC | 85-99      | 1884       |

// COMMAND ----------

// DBTITLE 1,Comprobación
dataAge.select($"edad").where($"edad">84).count()

// COMMAND ----------

// DBTITLE 1,PromedioEdad
import org.apache.spark.sql.functions._
val avgAge = dataAge.select(avg($"edad")).first()(0).asInstanceOf[Double]


// COMMAND ----------

// DBTITLE 1,desviación estandarEdad
import org.apache.spark.sql.functions._
val stddevAge = dataAge.select(stddev($"edad")).first()(0).asInstanceOf[Double]

// COMMAND ----------

// DBTITLE 1,Cálculo de Límites
val lowerLimit = avgAge -3* stddevAge 
val higherLimit = avgAge +3* stddevAge 

// COMMAND ----------

// DBTITLE 1,Datos menores al limite inferior 
val lowerAges = dataAge.where($"edad"< lowerLimit)
lowerAges.describe().show()

// COMMAND ----------

// DBTITLE 1,Datos mayores al limite superior 
val higherAges = dataAge.where($"edad"> higherLimit)
higherAges.describe().show()

// COMMAND ----------

dataAge.select($"edad").where($"edad" >87).count()

// COMMAND ----------

// DBTITLE 1,Creación de un dataFrame sin outliers
val dataAgesOutLimits = dataAge.where($"edad"> lowerLimit && $"edad"< higherLimit)

// COMMAND ----------

dataAgesOutLimits.summary().show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC |  Summary sin outliers   |
// MAGIC |-------------------------|
// MAGIC | **count**  | 622056     |
// MAGIC | **mean**   | 40.63      |
// MAGIC | **stv**    | 15.46      |
// MAGIC | **min**    | 15         |
// MAGIC | **max**    | 87         |
// MAGIC 
// MAGIC |  Summary con outliers   |
// MAGIC |-------------------------|
// MAGIC | **count**  | 622776     |
// MAGIC | **mean**   | 40.68      |
// MAGIC | **stv**    | 15.53      |
// MAGIC | **min**    | 15         |
// MAGIC | **max**    | 99         |
