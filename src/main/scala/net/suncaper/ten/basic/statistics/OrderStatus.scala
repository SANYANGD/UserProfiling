package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{avg, max, when}

class OrderStatus {
  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_orders"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_biz"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "avgOrderAmount":{"cf":"biz", "col":"avgOrderAmount", "type":"string"},
       |  "maxOrderAmount":{"cf":"biz", "col":"maxOrderAmount", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("OrderStatus")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  //客单价 1-999、1000-2999、3000-4999、5000-9999
  var result = source.groupBy('memberId).count()


  def orderStatusWrite = {

    source.show()
    result.show()

    try{

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("orderStatusWrite finish")

    }



    spark.close()

  }

}
