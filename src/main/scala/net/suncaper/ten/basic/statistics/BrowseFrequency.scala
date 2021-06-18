package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BrowseFrequency {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_logs"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "log_time":{"cf":"cf", "col":"log_time", "type":"string"},
       |    "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_log"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "browseFrequency":{"cf":"log", "col":"browseFrequency", "type":"string"}
       |}
       |}""".stripMargin
  
  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"browseFrequency",
       |"columns":{
       |"browseFrequency":{"cf":"rowkey", "col":"browseFrequency", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
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

  val sum = count(col("log_time")).as("sum")

  val logTime = datediff(max("log_time"),min("log_time")).as("logTime")

  var result = source
    .groupBy("global_user_id")
    .agg(sum,logTime)

  result = result.select($"global_user_id",$"sum".cast("int"), $"logTime".cast("int"),
    when(('sum/'logTime) === 0, "从不")
      .when((('sum/'logTime) > 0) and (('sum/'logTime) <= 7), "很少")
      .when((('sum/'logTime) > 7) and (('sum/'logTime) <= 14), "偶尔")
      .otherwise("经常")
    .as("browseFrequency"))
    .withColumnRenamed("global_user_id","id")
      .drop("logTime")
      .drop("sum")

  val finalBrowseFrequencyW = result
    .select('id,'browseFrequency)
    .groupBy('browseFrequency)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)


  def browseFrequencyWrite={
    source.show()
    result.show()
    finalBrowseFrequencyW.show()

    try{

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalBrowseFrequencyW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("browseFrequencyWrite finish")

    }

    spark.close()
  }

}
