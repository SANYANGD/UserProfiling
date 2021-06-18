package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

class LogFrequency {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_logs"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "log_time":{"cf":"cf", "col":"log_time", "type":"string"},
       |    "loc_url":{"cf":"cf", "col":"loc_url", "type":"string"},
       |    "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_log"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "logFrequency":{"cf":"log", "col":"logFrequency", "type":"string"}
       |}
       |}""".stripMargin


  val spark = SparkSession.builder()
    .appName("browseTime")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val log = datediff(max("log_time"),min("log_time")).as("log")

  val sum = count("loc_url").as("sum")

  var result = source
    .groupBy("global_user_id","loc_url")
    .agg(log, sum)
    .where(col("loc_url") like "%login.html")


  result = result.select($"global_user_id",$"sum".cast("int"), $"log".cast("int"),
      when($"sum" === 0, "从不")
        .when(($"sum"/($"log"+1)) <= 0.2, "较少")
        .when((($"sum"/($"log"+1)) > 0.2) and (($"sum"/($"log"+1)) <= 0.5), "一般")
        .when((($"sum"/($"log"+1)) > 0.5) and (($"sum"/($"log"+1)) <= 1), "经常")
        .otherwise("牛")
        .as("logFrequency")
    )
    .withColumnRenamed("global_user_id","id")
      .drop("sum")
      .drop("log")



  def logFrequencyWrite={
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

      println("logFrequencyWrite finish")

    }

    spark.close()
  }

}
