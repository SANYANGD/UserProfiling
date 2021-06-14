package net.suncaper.ten.basic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

class ConsumptionCycle {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_orders"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
       |  }
       |}""".stripMargin

//  def catalogWrite =
//    s"""{
//       |"table":{"namespace":"default", "name":"aft_basic_biz"},
//       |"rowkey":"id",
//       |"columns":{
//       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
//       |  "paymentCode":{"cf":"biz", "col":"modified", "type":"string"}
//       |}
//       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("ConsumptionCycle")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val result = source.select('memberId, 'finishTime).orderBy('memberId)

  def consumptionCycleWrite ={

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
