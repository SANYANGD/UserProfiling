package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class AgeGroup {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"birthday":{"cf":"cf", "col":"birthday", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"ageGroup":{"cf":"user", "col":"ageGroup", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"ageGroup",
       |"columns":{
       |"ageGroup":{"cf":"rowkey", "col":"ageGroup", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("shc test")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val ageGroupW = readDF
    .select('id,
      when(year($"birthday") < 1960 && year($"birthday") > 1949, "50后")
        .when(year($"birthday") < 1970 && year($"birthday") > 1959, "60后")
        .when(year($"birthday") < 1980 && year($"birthday") > 1969, "70后")
        .when(year($"birthday") < 1990 && year($"birthday") > 1979, "80后")
        .when(year($"birthday") < 2000 && year($"birthday") > 1989, "90后")
        .when(year($"birthday") < 2010 && year($"birthday") > 1999, "00后")
        .when(year($"birthday") < 2020 && year($"birthday") > 2009, "10后")
        .when(year($"birthday") < 2030 && year($"birthday") > 2019, "20后")
        .as("ageGroup")
    )

  val finalAgeGroupW = ageGroupW
    .select('id,'ageGroup)
    .groupBy('ageGroup)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  def ageGroupWrite={
    readDF.show()
    ageGroupW.show()
    finalAgeGroupW.show()

    try{

      ageGroupW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalAgeGroupW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("ageGroupW finish")

    }



    spark.close()
  }

}
