package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{format_number, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PoliticalFace {

  def catalogGenderRead =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"}
       |}
       |}""".stripMargin

  def catalogGenderWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"politicalFace":{"cf":"user", "col":"politicalFace", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"politicalFace",
       |"columns":{
       |"politicalFace":{"cf":"rowkey", "col":"politicalFace", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("politicalFace")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalogGenderRead)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val result = readDF.select('id,
    when('politicalFace === "1", "群众")
      .when('politicalFace === "2", "党员")
      .when('politicalFace === "3", "无党派人士")
      .otherwise("未知")
      .as("politicalFace"))

  val finalPoliticalFaceW = result
    .select('id,'politicalFace)
    .groupBy('politicalFace)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  def politicalFaceWrite = {

    readDF.show()
    result.show()
    finalPoliticalFaceW.show()

    try{

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogGenderWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalPoliticalFaceW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("politicalFaceWrite finish")

    }



    spark.close()
  }

}
