package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class LogSession {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_beh"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"LogTime":{"cf":"behavior", "col":"LogTime", "type":"string"},
       |"logSession":{"cf":"behavior", "col":"logSession", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"logSession",
       |"columns":{
       |"logSession":{"cf":"rowkey", "col":"logSession", "type":"string"},
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
 // val recencyCol = datediff(date_sub(current_timestamp(), 660), from_unixtime(max('finishTime))) as "temp"

  val logSessionW = readDF
    .select('id,from_unixtime('lastLoginTime) as 'LogTime)
      .select('id,'LogTime,
        when(hour($"LogTime") < 8 && hour($"LogTime") > -1, "0-7点")
        .when(hour($"LogTime") < 13 && hour($"LogTime") > 7, "8-12点")
        .when(hour($"LogTime") < 18 && hour($"LogTime") > 12, "13-17点")
        .when(hour($"LogTime") < 22 && hour($"LogTime") > 17, "18-21点")
        .when(hour($"LogTime") < 24 && hour($"LogTime") > 21, "22-24点")
        .as("logSession"))

  val finalLogSessionW = logSessionW
    .select('id,'logSession)
    .groupBy('logSession)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  def logSessionWrite={

    readDF.show()
    logSessionW.show()
    finalLogSessionW.show()

    try{

      logSessionW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalLogSessionW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("logSessionWrite finish")

    }



    spark.close()

  }

}
