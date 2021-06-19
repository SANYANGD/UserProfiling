package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Marriage {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"marriage":{"cf":"cf", "col":"marriage", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"marriage":{"cf":"user", "col":"marriage", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"marriage",
       |"columns":{
       |"marriage":{"cf":"rowkey", "col":"marriage", "type":"string"},
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

  //婚姻状况：1未婚，2已婚，3离异
  val marriageW = readDF.select('id,
    when('marriage === "1", "未婚")
      .when('marriage === "2", "已婚")
      .when('marriage === "3", "离异")
      .otherwise("其他")
      .as("marriage")
  )
  val finalMarriageW = marriageW
    .select('id,'marriage)
    .groupBy('marriage)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  def marriageWrite={

    readDF.show()
    marriageW.show()
    finalMarriageW.show()

    try{

      marriageW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("marriageW finish")

    }
    try{
      finalMarriageW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("marriageFinalW finish")

    }



    spark.close()
  }

}
