package net.suncaper.ten.basic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, current_timestamp, date_sub, datediff, from_unixtime, max, row_number, when}
import org.apache.spark.sql.sources.LessThanOrEqual

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

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_biz"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "consumptionCycle":{"cf":"biz", "col":"consumptionCycle", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("ConsumptionCycle")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  //消费周期
  //近7天 0-7
  //近2周 8-14
  //近1月 15-30
  //近2月 31-60
  //近3月 61-90
  //近4月 91-120
  //近5月 121-150
  //近半年 151-180
  val recencyCol = datediff(date_sub(current_timestamp(), 660), from_unixtime(max('finishTime))) as "temp"
  val result = source.groupBy('memberId).agg(recencyCol).
    select('*,
      when('temp <= 7 , "近7天").
        when('temp <= 14 and('temp > 7), "近2周").
        when('temp <= 30 and('temp > 14), "近1月").
        when('temp <= 60 and('temp > 30), "近2月").
        when('temp <= 90 and('temp > 60), "近3月").
        when('temp <= 120 and('temp > 90), "近4月").
        when('temp <= 150 and('temp > 120), "近5月").
        otherwise( "近半年").
        as('consumptionCycle))
    .withColumnRenamed("memberId", "id").drop("temp")

  def consumptionCycleWrite ={

    source.show()
    result.show()

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.close()
  }

}
