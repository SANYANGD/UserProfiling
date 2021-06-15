package net.suncaper.ten.basic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{current_timestamp, date_sub, datediff, from_unixtime, max, when}

class PaidAmount {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_orders"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |    "paidAmount":{"cf":"cf", "col":"paidAmount", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_biz"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "avgPaidAmount":{"cf":"biz", "col":"avgPaidAmount", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("paidAmiunt")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  //客单价 1-999、1000-2999、3000-4999、5000-9999
  val result = source.groupBy('memberId).select('*)

  def paidAmountWrite ={

    source.show()
    result.show()

//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

    spark.close()
  }

}
