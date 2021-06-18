package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PaidAmount {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_orders"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"}
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

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"maxOrderAmount",
       |"columns":{
       |"maxOrderAmount":{"cf":"rowkey", "col":"maxOrderAmount", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
       |}
       |}""".stripMargin

  def finalAvgWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"avgOrderAmount",
       |"columns":{
       |"avgOrderAmount":{"cf":"rowkey", "col":"avgOrderAmount", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
       |}
       |}""".stripMargin
  val spark = SparkSession.builder()
    .appName("OrderAmount")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  //客单价 1-999、1000-2999、3000-4999、5000-9999
  var result = source.select($"memberId", $"orderAmount".cast("int"))
    .groupBy('memberId)
    .agg(avg('orderAmount) as "avg", max('orderAmount) as "max")

  result = result.select('*,
    when('avg <= 999 and ('avg > 1), "单价：1-999").
      when('avg <= 2999 and ('avg > 999), "单价：1000-2999").
      when('avg <= 4999 and ('avg > 2999), "单价：3000-4999").
      when('avg <= 9999 and ('avg > 4999), "单价：5000-9999").
      otherwise("单价：10000-").as("avgOrderAmount")).drop("avg")

  result = result.select('*,
    when('max <= 999 and ('max > 1), "最高：1-999").
      when('max <= 2999 and ('max > 999), "最高：1000-2999").
      when('max <= 4999 and ('max > 2999), "最高：3000-4999").
      when('max <= 9999 and ('max > 4999), "最高：5000-9999").
      otherwise("最高：10000-").as("maxOrderAmount")).drop("max").
    withColumnRenamed("memberId", "id")
  
  val finalPaidAmountW = result
    .select('id,'maxOrderAmount)
    .groupBy('maxOrderAmount)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)

  val finalAvgPaidAmountW = result
    .select('id,'avgOrderAmount)
    .groupBy('avgOrderAmount)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)
  
  def paidAmountWrite = {

//    source.show()
//    result.show()
//    finalPaidAmountW.show()
//    finalAvgPaidAmountW.show()
    try{

//      result.write
//        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//        .option(HBaseTableCatalog.newTable, "5")
//        .format("org.apache.spark.sql.execution.datasources.hbase")
//        .save()

      finalPaidAmountW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

//      finalAvgPaidAmountW.write
//        .option(HBaseTableCatalog.tableCatalog, finalAvgWrite)
//        .option(HBaseTableCatalog.newTable, "5")
//        .format("org.apache.spark.sql.execution.datasources.hbase")
//        .save()
    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("PaidAmount finish")

    }



    spark.close()

  }

}
