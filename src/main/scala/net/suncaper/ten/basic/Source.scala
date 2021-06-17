package net.suncaper.ten.basic

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

class Source {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"source":{"cf":"cf", "col":"source", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"source":{"cf":"user", "col":"source", "type":"string"}
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


  val sourceW = readDF

  def sourceWrite={

    readDF.show()
    sourceW.show()

    try{

      sourceW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("sourceWrite finish")

    }



    spark.close()

  }

}
