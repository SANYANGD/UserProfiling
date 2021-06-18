package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{format_number, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Nationality {


  def catalogGenderRead =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"nationality":{"cf":"cf", "col":"nationality", "type":"string"}
       |}
       |}""".stripMargin

  def catalogGenderWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"nationality":{"cf":"user", "col":"nationality", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"nationality",
       |"columns":{
       |"nationality":{"cf":"rowkey", "col":"nationality", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("nationality")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalogGenderRead)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val result = readDF.select('id,
    when('nationality === "1", "中国大陆")
      .when('nationality === "2", "中国香港")
      .when('nationality === "3", "中国澳门")
      .when('nationality === "4", "中国台湾")
      .when('nationality === "5", "其他")
      .otherwise("未知")
      .as("nationality"))

  val finalNationalityW = result
    .select('id,'nationality)
    .groupBy('nationality)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  def nationalityWrite = {

    readDF.show()
    result.show()
    finalNationalityW.show()
    try{

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogGenderWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalNationalityW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()


    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("nationalityWrite finish")

    }



    spark.close()
  }

}
