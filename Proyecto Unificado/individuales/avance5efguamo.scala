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

data.show

// COMMAND ----------

// DBTITLE 1,Opcion 1
// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS PROVINCIA;
// MAGIC 
// MAGIC CREATE TABLE PROVINCIA
// MAGIC (codProv CHAR(2) NOT NULL,
// MAGIC provincia VARCHAR(30) NOT NULL
// MAGIC );
// MAGIC 
// MAGIC Insert Into provincia values('01','Azuay ');
// MAGIC Insert Into provincia values('02','Bolivar');
// MAGIC Insert Into provincia values('03','Cañar');
// MAGIC Insert Into provincia values('04','Carchi');
// MAGIC Insert Into provincia values('05','Cotopaxi');
// MAGIC Insert Into provincia values('06','Chimborazo');
// MAGIC Insert Into provincia values('07','El Oro');
// MAGIC Insert Into provincia values('08','Esmeraldas');
// MAGIC Insert Into provincia values('09','Guayas');
// MAGIC Insert Into provincia values('10','Imbabura');
// MAGIC Insert Into provincia values('11','Loja');
// MAGIC Insert Into provincia values('12','Los Ríos');
// MAGIC Insert Into provincia values('13','Manabi');
// MAGIC Insert Into provincia values('14','Morona Santiago');
// MAGIC Insert Into provincia values('15','Napo');
// MAGIC Insert Into provincia values('16','Pastaza');
// MAGIC Insert Into provincia values('17','Pichincha');
// MAGIC Insert Into provincia values('18','Tungurahua');
// MAGIC Insert Into provincia values('19','Zamora Chinchipe');
// MAGIC Insert Into provincia values('20','Galapagos');
// MAGIC Insert Into provincia values('21','Sucumbios');
// MAGIC Insert Into provincia values('22','Orellana');
// MAGIC Insert Into provincia values('23','Santo Domingo de los Tsachilas');
// MAGIC Insert Into provincia values('24','Santa Elena ');

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Opcion 2 

// COMMAND ----------

val provincesSchema = "idProv INT, ProvinciaS STRING"
val cantonesSchema = "idCanton INT, Cantones STRING"

// COMMAND ----------

val dataProvincias = spark
.read
.schema(provincesSchema)
.option("charset", "ISO-8859-1")
.option("header", "true")
.csv("/FileStore/tables/Provincias.csv")

// COMMAND ----------

val dataCantones = spark
.read
.schema(cantonesSchema)
.option("charset", "ISO-8859-1")
.option("header", "true")
.csv("/FileStore/tables/Cantones_1_.csv")

// COMMAND ----------

val aux = data.join(dataProvincias, data("provincia")=== dataProvincias("idProv"), "inner").join(dataCantones, data("canton") === dataCantones("idCanton"), "inner").drop("provincia", "idProv", "idCanton", "canton")
val dataD = aux.select("id","anio","mes","Provincias","Cantones","area","genero", "edad","estado_civil","nivel_de_instruccion","etnia","ingreso_laboral","condicion_actividad","sectorizacion","grupo_ocupacion", "rama_actividad","factor_expansion")
dataD.show

// COMMAND ----------

val numData = dataD.count().toDouble

// COMMAND ----------

// MAGIC %md
// MAGIC #Revision de Data 

// COMMAND ----------

dataD.select("ingreso_laboral", "edad").summary().show()

// COMMAND ----------

dataD.select("ingreso_laboral").groupBy("ingreso_laboral").count.sort($"ingreso_laboral".desc).show(5)
dataD.select("edad").groupBy("edad").count.sort($"edad".desc).show(5)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Como se puede visualizar existen valores muy altos en las columnas ingreso_labiral y anio que alteran el promedio, por lo que es necesario realizar una eliminación de *outliers*
// MAGIC #### Método de la Desviación Estandar 

// COMMAND ----------

val dfIngresoLaboral = dataD.where($"ingreso_laboral".isNotNull)
val dataAge = dataD.select("edad")

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


val avgCorrecto = ingresoLaboralSinOutliers.select($"ingreso_laboral").summary("mean")
avgCorrecto.show

// COMMAND ----------

val dataAgesOutLimits = dataAge.where($"edad"> lowerLimit && $"edad"< higherLimit)
dataAgesOutLimits.select($"edad").summary().show()

// COMMAND ----------

// DBTITLE 1,1. Agrupación de la información en función de las categorias existentes y la cantidad de personas pertenecientes a esta.  #E
import org.apache.spark.sql.functions._
display(dataD
.filter("grupo_ocupacion is not null")
.groupBy("grupo_ocupacion")
.agg(count("grupo_ocupacion").alias("Numero"))
.withColumn("aux", col("Numero")*100 /  dataD.count())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux")
.sort($"Numero".desc))

// COMMAND ----------

// DBTITLE 1,2. Número de personas que conforman cada grupo ocupación, divididas en años  #E
display(dataD.groupBy("anio").pivot("grupo_ocupacion").count().orderBy("anio"))


// COMMAND ----------

// DBTITLE 1,3. Número de personas que dejaron vacias la columna grupo_ocupacion y agrupada en Años #B
display(data
        .filter("grupo_ocupacion is null")
        .groupBy("anio").count()
        .withColumn("aux", col("count")*100 / sum("count").over())
        .withColumn("Porcentaje",round($"aux",2))
        .drop("aux")
        .sort($"anio"))

// COMMAND ----------

// DBTITLE 1,No pertenecen a un grupo y tienen sueldo
dataD.filter("grupo_ocupacion is null and ingreso_laboral > 0").groupBy("anio").count.show

// COMMAND ----------

// MAGIC %md
// MAGIC #### Durante los años 2015 y 2016 existen varios cientos de registros que aunque su grupo de ocupación es *nulo* tienen un ingreso laboral, por lo que se deduce que estas irregularidades son producto de ocupaciones no registradas/contempladas en los grupos originales y que estas fueron incluidas en años posteriores 

// COMMAND ----------

// DBTITLE 1,4. #B
display(data
        .filter("grupo_ocupacion is not null")
        .groupBy("grupo_ocupacion")
        .agg(sum("ingreso_laboral").alias("Ingreso nacional"))
        .withColumn("Porcentaje", col("Ingreso nacional")*100 /  sum("Ingreso nacional").over()).sort(desc("Porcentaje")))

// COMMAND ----------

// DBTITLE 1,5. #E
display(dataD.groupBy("Provincias").pivot("grupo_ocupacion").count().orderBy("Provincias"))

// COMMAND ----------

// DBTITLE 1,6. #E
display(dataD
        .groupBy("Provincias")
        .pivot("grupo_ocupacion")
        .avg("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// DBTITLE 1,Sin Outliers
display(ingresoLaboralSinOutliers
        .groupBy("Provincias")
        .pivot("grupo_ocupacion")
        .avg("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// DBTITLE 1,7. #B
display(ingresoLaboralSinOutliers
        .groupBy("Provincias")
        .pivot("anio")
        .sum("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// MAGIC %md 
// MAGIC # Cuantas Personas pueden pagar pagar la canasta basica ($716.14) 
// MAGIC [Precio actual de la canasta basica (pag 10)](https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/2020/Enero-2020/Boletin_tecnico_01-2020-IPC.pdf)

// COMMAND ----------

val CanastaBasica2019 = dataD.filter("anio =2019").withColumn("Valores",expr("ingreso_laboral > 716.14"))
val CanastaBasica2018 = dataD.filter("anio =2018").withColumn("Valores",expr("ingreso_laboral > 628.27"))
val CanastaBasica2017 = dataD.filter("anio =2017").withColumn("Valores",expr("ingreso_laboral > 709.3"))
val CanastaBasica2016 = dataD.filter("anio =2016").withColumn("Valores",expr("ingreso_laboral > 675.93"))
val CanastaBasica2015 = dataD.filter("anio =2015").withColumn("Valores",expr("ingreso_laboral > 553.21"))


// COMMAND ----------

// MAGIC %md
// MAGIC # 8 #E

// COMMAND ----------

display(CanastaBasica2019
        .groupBy("valores").count())

// COMMAND ----------

display(CanastaBasica2018
        .groupBy("valores").count())

// COMMAND ----------

display(CanastaBasica2017
        .groupBy("valores").count())

// COMMAND ----------

display(CanastaBasica2016
        .groupBy("valores").count())


// COMMAND ----------

display(CanastaBasica2015
        .groupBy("valores").count())

// COMMAND ----------

// MAGIC %md
// MAGIC # 9  #E

// COMMAND ----------

// MAGIC %md
// MAGIC #10  #B

// COMMAND ----------


