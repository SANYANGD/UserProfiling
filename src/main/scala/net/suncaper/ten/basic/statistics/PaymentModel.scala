package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class PaymentModel {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_orders"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |    "paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_biz"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "paymentCode":{"cf":"biz", "col":"paymentCode", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("PaymentModel")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val result = source.groupBy('memberId, 'paymentCode)
    .agg(count('paymentCode) as "count")
    .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
    .where('row_num === 1)
    .withColumnRenamed("memberId", "id").drop("count", "row_num")
  //val result = source.select('memberId, 'paymentCode).orderBy('memberId)

  def payModelWrite ={

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

      println("payModelWrite finish")

    }


    spark.close()

  }
}
