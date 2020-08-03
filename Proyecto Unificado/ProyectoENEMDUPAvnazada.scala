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

// DBTITLE 1,Revision de la Información 
data.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Las columnas provincia y canton son de tipo numérica, por lo que es difícil identificar la información, por lo que se debe traducir
// MAGIC ### Para eso existe dos opciones 
// MAGIC ### 1) Usando las funciones sql de spark y generar tablas para posteriormente realizar un cambio 
// MAGIC ### 2) Exportando archivos CSV con el nombre de las provincias/cantones con su respectivo codigo, transformarlos a dataframes y posteriormente realizar el cambio

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

// DBTITLE 1,Cáculo del promedio, desviación estandar y limites 
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

// DBTITLE 1,Obtención del promedio ingreso_laboral sin outliers 
val ingresoLaboralSinOutliers = dfIngresoLaboral.where($"ingreso_laboral">inferior && $"ingreso_laboral" < superior)

val avgCorrecto = ingresoLaboralSinOutliers.select($"ingreso_laboral").summary("mean")
avgCorrecto.show

// COMMAND ----------

// DBTITLE 1,Obtención del promedio edad sin outliers 
val dataAgesOutLimits = dataAge.where($"edad"> lowerLimit && $"edad"< higherLimit)
dataAgesOutLimits.select($"edad").summary().show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Una vez depurada la infomración del dataFrame se realizará las consultas 

// COMMAND ----------

// DBTITLE 1,Consulta N1: Agrupación de la información en los diferentes grupos de ocupación
display(dataD
.filter("grupo_ocupacion is not null")
.groupBy("grupo_ocupacion")
.agg(count("grupo_ocupacion").alias("Numero"))
.withColumn("aux", col("Numero")*100 /  dataD.count())
.withColumn("Porcentaje",round($"aux",2))
.drop("aux").sort($"Porcentaje".desc))

// COMMAND ----------

// DBTITLE 1,Consulta N2: Número de personas que conforman cada grupo ocupación divididas en años 
display(dataD.groupBy("anio").pivot("grupo_ocupacion").count().orderBy("anio"))


// COMMAND ----------

// DBTITLE 1,Consulta N3: Número de personas que dejaron vacias la columna grupo_ocupacion y agrupada en Años
display(data
        .filter("grupo_ocupacion is null")
        .groupBy("anio").count()
        .withColumn("aux", col("count")*100 / sum("count").over())
        .withColumn("Porcentaje",round($"aux",2))
        .drop("aux")
        .sort($"anio"))

// COMMAND ----------

// DBTITLE 1,Consulta Complementaria 1: Busqueda de personas que tienen un ingreso_laboral, pero no estan asignadas a un grupo
display(dataD.filter("grupo_ocupacion is null and ingreso_laboral > 0").groupBy("anio").count())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Durante los años 2015 y 2016 existen varios cientos de registros que aunque su grupo de ocupación es *nulo* tienen un ingreso laboral, por lo que se deduce que estas irregularidades son producto de ocupaciones no registradas/contempladas en los grupos originales y que estas fueron incluidas en años posteriores 

// COMMAND ----------

// DBTITLE 1,Consulta N4: Ingreso laboral generado por los Grupos de Ocupación 
display(data
        .filter("grupo_ocupacion is not null")
        .groupBy("grupo_ocupacion")
        .agg(sum("ingreso_laboral").alias("Ingreso nacional"))
        .withColumn("Porcentaje", col("Ingreso nacional")*100 /  sum("Ingreso nacional").over()).sort(desc("Porcentaje")))

// COMMAND ----------

// DBTITLE 1,Consulta N5: Distribución de los Grupos de Ocupación según las provincias 
display(dataD.groupBy("Provincias").pivot("grupo_ocupacion").count().orderBy("Provincias"))

// COMMAND ----------

// DBTITLE 1,Consulta Complementaria 2: Promedio de Ingreso Laboral por provincia con outliers
display(dataD
        .groupBy("Provincias")
        .pivot("grupo_ocupacion")
        .avg("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// DBTITLE 1,Consulta N6: Promedio de Ingreso Laboral por provincia sin outliers 
display(ingresoLaboralSinOutliers
        .groupBy("Provincias")
        .pivot("grupo_ocupacion")
        .avg("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// DBTITLE 1,Consulta N7: Dinero generado por provincia durante el periodo 2015-2019 
display(ingresoLaboralSinOutliers
        .groupBy("Provincias")
        .pivot("anio")
        .sum("ingreso_laboral")
        .sort(asc("Provincias")))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Consulta N8:Personas que pueden cubrir el valor de la canasta básica Familiar por anio 

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Cuantas Personas pueden pagar pagar la canasta basica 
// MAGIC 
// MAGIC [Canasta Familiar Básica 2015: $553.21](https://www.ecuadorencifras.gob.ec//documentos/web-inec/Inflacion/canastas/Canastas_2015/Enero/1.%20Informe_Ejecutivo_Canastas_Analiticas_ene2015.pdf/)
// MAGIC 
// MAGIC [Canasta Familiar Básica 2016: 675.93](https://www.ecuadorencifras.gob.ec//documentos/web-inec/Inflacion/canastas/Canastas_2016/Enero/1.%20Informe_Ejecutivo_Canastas_Analiticas_ene2016.pdf)
// MAGIC 
// MAGIC [Canasta Familiar Básica 2017: 709.30](https://www.eluniverso.com/noticias/2017/02/07/nota/6036493/canasta-basica-enero-fue-7019)
// MAGIC 
// MAGIC [Canasta Familiar Básica 2018: 628.27](https://www.elcomercio.com/actualidad/negocios/ingreso-familiar-supera-costo-de.html)
// MAGIC 
// MAGIC [Canasta Familiar Básica 2019: 716.14(pag 10)](https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/2020/Enero-2020/Boletin_tecnico_01-2020-IPC.pdf)

// COMMAND ----------

val CanastaBasica2019 = dataD.filter("anio =2019").withColumn("2019",expr("ingreso_laboral > 716.14"))
val CanastaBasica2018 = dataD.filter("anio =2018").withColumn("2018",expr("ingreso_laboral > 628.27"))
val CanastaBasica2017 = dataD.filter("anio =2017").withColumn("2017",expr("ingreso_laboral > 709.3"))
val CanastaBasica2016 = dataD.filter("anio =2016").withColumn("2016",expr("ingreso_laboral > 675.93"))
val CanastaBasica2015 = dataD.filter("anio =2015").withColumn("2015",expr("ingreso_laboral > 553.21"))


// COMMAND ----------

display(CanastaBasica2019
        .groupBy("2019").count())

// COMMAND ----------

display(CanastaBasica2018
        .groupBy("2018").count())

// COMMAND ----------

display(CanastaBasica2017
        .groupBy("2017").count())

// COMMAND ----------

display(CanastaBasica2016
        .groupBy("2016").count())


// COMMAND ----------

display(CanastaBasica2015
        .groupBy("2015").count())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Consulta N9: Porcentaje de las personas que pueden solventar el precio de la canasta Báscia en las 3 ciudades más caras del Ecuador 
// MAGIC [Ciudades más caras del Ecuador](https://www.eluniverso.com/noticias/2020/01/07/nota/7679187/loja-ciudad-mas-cara-ecuador-inec-2019)

// COMMAND ----------

val canastaBasicaLoja2019 = data.filter("anio = 2019 and canton = 1101").withColumn("Loja", expr("ingreso_laboral > 747.77"))
val canastaBasicaGuayaquil2019 = data.filter("anio = 2019 and canton = 0901").withColumn("Guayaquil", expr("ingreso_laboral > 730.24"))
val canastaBasicaCuenca2019 = data.filter("anio = 2019 and canton = 0101").withColumn("Cuenca", expr("ingreso_laboral > 736.44"))

// COMMAND ----------

display(canastaBasicaLoja2019.groupBy("Loja").count)

// COMMAND ----------

display(canastaBasicaGuayaquil2019.groupBy("Guayaquil").count)


// COMMAND ----------

display(canastaBasicaCuenca2019.groupBy("Cuenca").count)


// COMMAND ----------

// MAGIC %md 
// MAGIC ### Consulta N10:Porcentaje de las personas que recibin más del sueldo mínimo unificado 
// MAGIC [Sueldo Básico Unificado 2019: 354](http://www.ecuadorlegalonline.com/laboral/salario-basico-unificado-2019-sueldo/)
// MAGIC 
// MAGIC [Sueldo Básico Unificado 2018: 366](http://www.ecuadorlegalonline.com/laboral/salario-basico-2018/)
// MAGIC 
// MAGIC [Sueldo Básico Unificado 2017: 375](http://www.ecuadorlegalonline.com/laboral/salario-basico-2017/)
// MAGIC 
// MAGIC [Sueldo Básico Unificado 2016: 386](http://www.ecuadorlegalonline.com/laboral/salario-basico-2016/)
// MAGIC 
// MAGIC [Sueldo Básico Unificado 2015: 394](http://www.ecuadorlegalonline.com/laboral/salario-basico-2015/)

// COMMAND ----------

val salarioBasico2015 = data.filter("anio = 2015").withColumn("2015",expr("ingreso_laboral > 354"))
val salarioBasico2016 = data.filter("anio = 2016").withColumn("2016",expr("ingreso_laboral > 366"))
val salarioBasico2017 = data.filter("anio = 2017").withColumn("2017",expr("ingreso_laboral > 375"))
val salarioBasico2018 = data.filter("anio = 2018").withColumn("2018",expr("ingreso_laboral > 386"))
val salarioBasico2019 = data.filter("anio = 2019").withColumn("2019",expr("ingreso_laboral > 394"))

// COMMAND ----------

display(salarioBasico2015.groupBy("2015").count)

// COMMAND ----------

display(salarioBasico2016.groupBy("2016").count)

// COMMAND ----------

display(salarioBasico2017.groupBy("2017").count)

// COMMAND ----------

display(salarioBasico2018.groupBy("2018").count)

// COMMAND ----------

display(salarioBasico2019.groupBy("2019").count)

// COMMAND ----------


