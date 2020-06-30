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

data.summary().show()

// COMMAND ----------

//obtención del número total de datos en la columna grupo_ocupacion que no sean nulos 
val tamanioRealGrup = data.filter("grupo_ocupacion is not null").count


// COMMAND ----------

//Agrupación de la información en función de las categorias existentes y la cantidad de personas pertenecientes a esta. 
data.groupBy("grupo_ocupacion").count.sort($"count".desc).show(false);

// COMMAND ----------

val trabajAlto = (data.where($"grupo_ocupacion" === "09 - Trabajadores no calificados, ocupaciones elementales").count / tamanioRealGrup.toDouble)*100
val trabajAlto2 = (data.where($"grupo_ocupacion" === "10 - Fuerzas Armadas").count / tamanioRealGrup.toDouble)*100


// COMMAND ----------

//Edad más común entre las personas que han llenado la columna grupo_ocupacion
data.select($"grupo_ocupacion",$"edad").filter("grupo_ocupacion is not null").groupBy("edad").count().sort($"count".desc).show(10)

// COMMAND ----------

//Número de personas que conforman cada grupo ocupación, divididas en años 
print("09 - Trabajadores no calificados, ocupaciones elementales \n")
data.select($"anio").where($"grupo_ocupacion" === "09 - Trabajadores no calificados, ocupaciones elementales").groupBy("anio").count().sort($"anio".desc).show()
print("05 - Trabajad. de los servicios y comerciantes \n")
data.select($"anio").where($"grupo_ocupacion" === "05 - Trabajad. de los servicios y comerciantes").groupBy("anio").count().sort($"anio".desc).show()
print("06 - Trabajad. calificados agropecuarios y pesqueros \n")
data.select($"anio").where($"grupo_ocupacion" === "06 - Trabajad. calificados agropecuarios y pesqueros").groupBy("anio").count().sort($"anio".desc).show()
print("07 - Oficiales operarios y artesanos \n")
data.select($"anio").where($"grupo_ocupacion" === "07 - Oficiales operarios y artesanos").groupBy("anio").count().sort($"anio".desc).show()
print("02 - Profesionales científicos e intelectuales \n")
data.select($"anio").where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales").groupBy("anio").count().sort($"anio".desc).show()
print("08 - Operadores de instalac. máquinas y montad. \n")
data.select($"anio").where($"grupo_ocupacion" === "08 - Operadores de instalac. máquinas y montad.").groupBy("anio").count().sort($"anio".desc).show()
print("03 - Técnicos y profesionales de nivel medio\n ")
data.select($"anio").where($"grupo_ocupacion" === "03 - Técnicos y profesionales de nivel medio").groupBy("anio").count().sort($"anio".desc).show()
print("04 - Empleados de oficina \n")
data.select($"anio").where($"grupo_ocupacion" === "04 - Empleados de oficina").groupBy("anio").count().sort($"anio".desc).show()
print("01 - Personal direct./admin. pública y empresas \n")
data.select($"anio").where($"grupo_ocupacion" === "01 - Personal direct./admin. pública y empresas").groupBy("anio").count().sort($"anio".desc).show()
print("10 - Fuerzas Armadas \n")
data.select($"anio").where($"grupo_ocupacion" === "10 - Fuerzas Armadas").groupBy("anio").count().sort($"anio".desc).show()

// COMMAND ----------

//Número de personas que dejaron vacias la columna grupo_ocupacion y agrupada en Años
print("null \n")
data.select($"anio").filter("grupo_ocupacion is null").groupBy("anio").count().sort($"anio".desc).show()

// COMMAND ----------

// No pertenecen a un grupo y tienen sueldo
data.filter("grupo_ocupacion is null and ingreso_laboral > 0").groupBy("anio").count.show

// COMMAND ----------

//Frecuencia de las 3 ramas más habituales en relación a los tres grupos de agrupación más grandes 
print("09 - Trabajadores no calificados, ocupaciones elementales \n")
data.select($"grupo_ocupacion", $"rama_actividad").where($"grupo_ocupacion"==="09 - Trabajadores no calificados, ocupaciones elementales").groupBy("rama_actividad").count.sort($"count".desc).show(3,false)
print("05 - Trabajad. de los servicios y comerciantes \n")
data.select($"grupo_ocupacion", $"rama_actividad").where($"grupo_ocupacion"==="05 - Trabajad. de los servicios y comerciantes").groupBy("rama_actividad").count.sort($"count".desc).show(3,false)
print("07 - Oficiales operarios y artesanos \n")
data.select($"grupo_ocupacion", $"rama_actividad").where($"grupo_ocupacion"==="07 - Oficiales operarios y artesanos").groupBy("rama_actividad").count.sort($"count".desc).show(3,false)

// COMMAND ----------

//Frecuncia de las personas según el grupo de ocupación y a un anio determinado 
data.select($"edad",$"grupo_ocupacion").where($"edad"==="25").groupBy("grupo_ocupacion").count.sort($"count".desc).show(false)
data.select($"edad",$"grupo_ocupacion").where($"edad"==="30").groupBy("grupo_ocupacion").count.sort($"count".desc).show(false)
data.select($"edad",$"grupo_ocupacion").where($"edad"==="35").groupBy("grupo_ocupacion").count.sort($"count".desc).show(false)

// COMMAND ----------

// Trabajos de mayores a 65 años
data.where($"edad" >= 65).groupBy("rama_actividad").count.sort($"count"desc).show(5, false)

// COMMAND ----------

// Grupo ocupacion y provincia
// Una sola tabla con todas las provincias
data.groupBy("provincia", "grupo_ocupacion").count.sort($"provincia", $"count"desc).show(false)

// Una tabla de cada provincia
println("Azuay\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 01").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Bolivar\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 02").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Cañar\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 03").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Carchi\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 04").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Cotopaxi\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 05").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Chimborazo\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 06").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("El Oro\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 07").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Esmeraldas\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 08").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Guayas\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 09").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Imbabura\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 10").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Loja\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 11").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Los Rios\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 12").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Manabi\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 13").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Morona Santiago\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 14").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Napo\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 15").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Pastaza\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 16").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Pichincha\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 17").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Tungurahua\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 18").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Zamora Chinchipe\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 19").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Galapagos\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 20").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Sucumbios\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 21").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Orellana\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 22").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Santo Domingo de los Tsachilas\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 23").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Santa Elena\n")
data.select($"provincia", $"grupo_ocupacion").filter("provincia like 24").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)

// COMMAND ----------

// Cantones mas caros
println("Cuenca\n")
data.filter("canton like 0101").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Loja\n")
data.filter("canton like 1101").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)
println("Guayaquil\n")
data.filter("canton like 0901").groupBy("grupo_ocupacion").count.sort($"count"desc).show(3, false)

// COMMAND ----------


